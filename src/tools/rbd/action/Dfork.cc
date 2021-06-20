// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/types.h"
#include "common/errno.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace rbd {
namespace action {
namespace dfork {

namespace at = argument_types;
namespace po = boost::program_options;

int do_clone(librbd::RBD &rbd, librados::IoCtx &p_ioctx,
             const char *p_name, const char *p_snapname,
             librados::IoCtx &c_ioctx, const char *c_name,
             librbd::ImageOptions& opts) {
  return rbd.clone3(p_ioctx, p_name, p_snapname, c_ioctx, c_name, opts);
}

int do_protect_snap(librbd::Image& image, const char *snapname)
{
  int r = image.snap_protect(snapname);
  if (r < 0)
    return r;

  return 0;
}

int do_add_snap(librbd::Image& image, const char *snapname,
                uint32_t flags, bool no_progress)
{
  utils::ProgressContext pc("Creating snap", no_progress);
  
  int r = image.snap_create2(snapname, flags, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }

  pc.finish();
  return 0;
}

void get_create_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_snap_create_options(options);
  at::add_no_progress_option(options);
}

int execute_create(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  std::string dfork_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &dfork_name, true, utils::SNAPSHOT_PRESENCE_REQUIRED,
    utils::SPEC_VALIDATION_SNAP);
  if (r < 0) {
    return r;
  }

  uint32_t flags;
  r = utils::get_snap_create_flags(vm, &flags);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, namespace_name, image_name, "", "",
                                 false, &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  snap_name = image_name + "-snap";
  r = do_add_snap(image, snap_name.c_str(), flags,
                  vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    cerr << "rbd: failed to create snapshot: " << cpp_strerror(r)
         << std::endl;
    return r;
  }

  /* protect the snap */
  bool is_protected = false;
  r = image.snap_is_protected(snap_name.c_str(), &is_protected);
  if (r < 0) {
    std::cerr << "rbd: protecting snap failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  } else if (is_protected) {
    return 0;
  }

  r = do_protect_snap(image, snap_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: protecting snap failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  /* clone from the snap */
  librbd::ImageOptions opts;
  r = utils::get_image_options(vm, false, &opts);
  if (r < 0) {
    return r;
  }
  opts.set(RBD_IMAGE_OPTION_FORMAT, static_cast<uint64_t>(2));

  // duplicate 
  // librados::Rados rados;
  // librados::IoCtx io_ctx;
  // r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  // if (r < 0) {
  //   return r;
  // }

  librados::IoCtx dst_io_ctx;
  r = utils::init_io_ctx(rados, pool_name, namespace_name, &dst_io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = do_clone(rbd, io_ctx, image_name.c_str(), snap_name.c_str(), dst_io_ctx,
               dfork_name.c_str(), opts);
  if (r == -EXDEV) {
    std::cerr << "rbd: clone v2 required for cross-namespace clones."
              << std::endl;
    return r;
  } else if (r < 0) {
    std::cerr << "rbd: clone error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

static int do_set_dfork_dirty_by_id(const std::string &pool_name, 
                                    const std::string &namespace_name, 
                                    const std::string &image_id,
                                    uint8_t dirty) {
  librados::Rados rados; 
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.set_dfork_dirty(io_ctx, image_id.c_str(), dirty);
  if (r < 0) {
    std::cerr << "rbd: error setting the dfork dirty bit: "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

void get_set_dirty_arguments(po::options_description *positional,
                          po::options_description *options) {
  positional->add_options()
    (at::IMAGE_ID.c_str(), "image id\n(example: [<pool-name>/[<namespace>/]]<image-id>)");
  options->add_options()
    ("set", po::bool_switch(), "set the dirty bit");
  options->add_options()
    ("clear", po::bool_switch(), "clear the dirty bit");
}

int execute_set_dirty(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_id;
  bool is_set, is_clear;

  int r = utils::get_pool_image_id(vm, &arg_index, &pool_name, &namespace_name,
                                   &image_id);
  if (r < 0) {
    return r;
  }

  is_set = vm["set"].as<bool>();
  is_clear = vm["clear"].as<bool>();
  if (!is_set && !is_clear) {
    std::cerr << "rbd: no set/clear action specified" << std::endl;
    return -EINVAL;
  }
  if (is_set && is_clear) {
    std::cerr << "rbd: both set and clear actions specified" << std::endl;
    return -EINVAL;
  }

  r = do_set_dfork_dirty_by_id(pool_name, namespace_name, image_id, 
                               is_set?1:0);
  if (r < 0) {
    std::cerr << "rbd: set dfork dirty error: " << cpp_strerror(r) << std::endl;
    return -r;
  }

  return 0;
}

Shell::Action action_create(
  {"dfork", "create"}, {}, "dfork a disk image.", "",
  &get_create_arguments, &execute_create);
Shell::Action action_set_dirty(
  {"dfork", "dirty"}, {}, "Set the dfork dirty bit", "",
  &get_set_dirty_arguments, &execute_set_dirty);

} // namespace dfork
} // namespace action
} // namespace rbd
