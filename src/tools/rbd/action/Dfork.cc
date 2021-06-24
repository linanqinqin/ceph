// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/types.h"
#include "common/errno.h"
#include "include/stringify.h"
#include <iostream>
#include <boost/program_options.hpp>

#define SNAP_SUFFIX "-snap"

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

namespace {

bool is_auto_delete_snapshot(librbd::Image* image,
                             const librbd::snap_info_t &snap_info) {
  librbd::snap_namespace_type_t namespace_type;
  int r = image->snap_get_namespace_type(snap_info.id, &namespace_type);
  if (r < 0) {
    return false;
  }

  switch (namespace_type) {
  case RBD_SNAP_NAMESPACE_TYPE_TRASH:
    return true;
  default:
    return false;
  }
}

} // anonymous namespace

static int do_delete(librbd::RBD &rbd, librados::IoCtx& io_ctx,
                     const char *imgname, bool no_progress)
{
  utils::ProgressContext pc("Removing image", no_progress);
  int r = rbd.remove_with_progress(io_ctx, imgname, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }
  pc.finish();
  return 0;
}

int do_unprotect_snap(librbd::Image& image, const char *snapname)
{
  int r = image.snap_unprotect(snapname);
  if (r < 0)
    return r;

  return 0;
}

int do_remove_snap(librbd::Image& image, const char *snapname, bool force,
       bool no_progress)
{
  uint32_t flags = force? RBD_SNAP_REMOVE_FORCE : 0;
  int r = 0;
  utils::ProgressContext pc("Removing snap", no_progress);

  r = image.snap_remove2(snapname, flags, pc);
  if (r < 0) {
    pc.fail();
    return r;
  }

  pc.finish();
  return 0;
}

static int do_set_dfork_dirty_by_id(const std::string &pool_name, 
                                    const std::string &namespace_name, 
                                    const std::string &image_name,
                                    const std::string &image_id,
                                    uint8_t dirty) {
  librados::Rados rados; 
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.set_dfork_dirty(io_ctx, image_name.c_str(), image_id.c_str(), dirty);
  if (r < 0) {
    std::cerr << "rbd: error setting the dfork dirty bit: "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

static int do_check_dfork_dirty(const std::string &pool_name, 
                                const std::string &namespace_name, 
                                const std::string &image_name,
                                const std::string &image_id,
                                uint8_t *dirty,
                                bool block_on_clean) {
  librados::Rados rados; 
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.check_dfork_dirty(io_ctx, image_name.c_str(), image_id.c_str(), 
                            dirty, block_on_clean);
  if (r < 0) {
    std::cerr << "rbd: error setting the dfork dirty bit: "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

// static int do_check_dfork_dirty(librbd::Image *image,
//                                 const std::string &pool_name, 
//                                 const std::string &namespace_name, 
//                                 const std::string &image_name,
//                                 const std::string &image_id,
//                                 bool block_on_clean) {
//   librados::Rados rados; 
//   librados::IoCtx io_ctx;
//   int r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
//   if (r < 0) {
//     return r;
//   }

//   librbd::RBD rbd;
//   r = rbd.check_dfork_dirty(io_ctx, *image, image_name.c_str(), 
//                             image_id.c_str(), block_on_clean);
//   if (r < 0) {
//     std::cerr << "rbd: error setting the dfork dirty bit: "
//               << cpp_strerror(r) << std::endl;
//     return r;
//   }
//   return 0;
// }

static int do_unblock_dfork_dirty(const std::string &pool_name, 
                                  const std::string &namespace_name, 
                                  const std::string &image_name,
                                  const std::string &image_id) {
  librados::Rados rados; 
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.unblock_dfork_dirty(io_ctx, image_name.c_str(), image_id.c_str());
  if (r < 0) {
    std::cerr << "rbd: error unblocking dfork dirty updates: "
              << cpp_strerror(r) << std::endl;
    return r;
  }
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
  std::string image_id;
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
    std::cerr << "rbd: failed to open image" << cpp_strerror(r)
              << std::endl;
    return r;
  }

  /* get the image id */
  // uint8_t old_format;
  // r = image.old_format(&old_format);
  // if (r < 0) {
  //   std::cerr << "rbd: image format not supported" << cpp_strerror(r)
  //             << std::endl;
  //   return r;
  // }
  // if (old_format) {
  //   std::cerr << "rbd: image format not supported" << cpp_strerror(r)
  //             << std::endl;
  //   return r;
  // } 
  // else {
  //   r = image.get_id(&image_id);
  //   if (r < 0) {
  //     std::cerr << "rbd: failed to get image id" << cpp_strerror(r)
  //               << std::endl;
  //     return r;
  //   }
  // }

  /* clear the dfork dirty bit */
  uint8_t dirty;
  r = image.dirty(&dirty);
  if (r < 0)
    return r;
  if (dirty) {
    r = do_set_dfork_dirty_by_id(pool_name, namespace_name, image_name, image_id, 0);
    if (r < 0) {
      std::cerr << "rbd: set dfork dirty error: " << cpp_strerror(r) << std::endl;
      return -r;
    }
  }

  /* create the snapshot */
  snap_name = image_name + SNAP_SUFFIX;
  r = do_add_snap(image, snap_name.c_str(), flags,
                  vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: failed to create snapshot: " << cpp_strerror(r)
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

void get_remove_arguments(po::options_description *positional,
                          po::options_description *options) {
  at::add_snap_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_snap_create_options(options);
  at::add_no_progress_option(options);
}

int execute_remove(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  // std::string image_id;
  std::string snap_name;
  std::string dfork_name;
  bool no_progress = vm[at::NO_PROGRESS].as<bool>();

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &dfork_name, true, utils::SNAPSHOT_PRESENCE_REQUIRED,
    utils::SPEC_VALIDATION_SNAP);
  if (r < 0) {
    return r;
  }

  /* remove the cloned (dfork-ed) disk */
  {
  librados::Rados rados;
  librados::IoCtx io_ctx;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  io_ctx.set_pool_full_try();

  librbd::RBD rbd;
  r = do_delete(rbd, io_ctx, dfork_name.c_str(), no_progress);
  if (r < 0) {
    if (r == -ENOTEMPTY) {
      librbd::Image image;
      std::vector<librbd::snap_info_t> snaps;
      int image_r = utils::open_image(io_ctx, dfork_name, true, &image);
      if (image_r >= 0) {
        image_r = image.snap_list(snaps);
      }
      if (image_r >= 0) {
        snaps.erase(std::remove_if(snaps.begin(), snaps.end(),
                 [&image](const librbd::snap_info_t& snap) {
                                     return is_auto_delete_snapshot(&image,
                                                                    snap);
                                   }),
                    snaps.end());
      }

      if (!snaps.empty()) {
        std::cerr << "rbd: image has snapshots - these must be deleted"
                  << " with 'rbd snap purge' before the image can be removed."
                  << std::endl;
      } else {
        std::cerr << "rbd: image has snapshots with linked clones - these must "
                  << "be deleted or flattened before the image can be removed."
                  << std::endl;
      }
    } else if (r == -EBUSY) {
      std::cerr << "rbd: error: image still has watchers"
                << std::endl
                << "This means the image is still open or the client using "
                << "it crashed. Try again after closing/unmapping it or "
                << "waiting 30s for the crashed client to timeout."
                << std::endl;
    } else if (r == -EMLINK) {
      librbd::Image image;
      int image_r = utils::open_image(io_ctx, dfork_name, true, &image);
      librbd::group_info_t group_info;
      if (image_r == 0) {
  image_r = image.get_group(&group_info, sizeof(group_info));
      }
      if (image_r == 0) {
        std::string pool_name = "";
        librados::Rados rados(io_ctx);
        librados::IoCtx pool_io_ctx;
        image_r = rados.ioctx_create2(group_info.pool, pool_io_ctx);
        if (image_r < 0) {
          pool_name = "<missing group pool " + stringify(group_info.pool) + ">";
        } else {
          pool_name = pool_io_ctx.get_pool_name();
        }
        std::cerr << "rbd: error: image belongs to a group "
                  << pool_name << "/";
        if (!io_ctx.get_namespace().empty()) {
          std::cerr << io_ctx.get_namespace() << "/";
        }
        std::cerr << group_info.name;
      } else
  std::cerr << "rbd: error: image belongs to a group";

      std::cerr << std::endl
    << "Remove the image from the group and try again."
    << std::endl;
      image.close();
    } else {
      std::cerr << "rbd: delete error: " << cpp_strerror(r) << std::endl;
    }
    return r;
  }
  }

  /* unprotect the snapshot */
  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  io_ctx.set_pool_full_try();

  r = utils::open_image(io_ctx, image_name, false, &image);
  if (r < 0) {
    return r;
  }

  bool is_protected = false;
  snap_name = image_name + SNAP_SUFFIX;
  r = image.snap_is_protected(snap_name.c_str(), &is_protected);
  if (r < 0) {
    std::cerr << "rbd: unprotecting snap failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  } else if (!is_protected) {
    std::cerr << "rbd: snap is already unprotected" << std::endl;
    return -EINVAL;
  }

  r = do_unprotect_snap(image, snap_name.c_str());
  if (r < 0) {
    std::cerr << "rbd: unprotecting snap failed: " << cpp_strerror(r)
              << std::endl;
    return r;
  }

  /* remove the snapshot */
  r = do_remove_snap(image, snap_name.c_str(), false, no_progress);

  if (r < 0) {
    if (r == -EBUSY) {
      std::cerr << "rbd: snapshot "
                << std::string("'") + snap_name + "'"
                << " is protected from removal." << std::endl;
    } else {
      std::cerr << "rbd: failed to remove snapshot: " << cpp_strerror(r)
                << std::endl;
    }
    return r;
  }

  return 0;
}

void get_set_dirty_arguments(po::options_description *positional,
                          po::options_description *options) {
  // positional->add_options()
  //   (at::IMAGE_ID.c_str(), "image id\n(example: [<pool-name>/[<namespace>/]]<image-id>)");
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
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
  std::string image_name;
  std::string snap_name;
  std::string image_id;
  bool is_set, is_clear;

  // deprecated
  // int r = utils::get_pool_image_id(vm, &arg_index, &pool_name, &namespace_name,
  //                                  &image_id);
  // if (r < 0) {
  //   return r;
  // }

  if (vm.count(at::IMAGE_ID)) {
    image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, image_id.empty(),
    utils::SNAPSHOT_PRESENCE_PERMITTED, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  if (!image_id.empty() && !image_name.empty()) {
    std::cerr << "rbd: trying to access image using both name and id. "
              << std::endl;
    return -EINVAL;
  }
  if (!snap_name.empty()) {
    std::cerr << "rbd: snapshot not supported. "
              << std::endl;
    return -EINVAL;
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

  r = do_set_dfork_dirty_by_id(pool_name, namespace_name, image_name, image_id, 
                               is_set?1:0);
  if (r < 0) {
    std::cerr << "rbd: set dfork dirty error: " << cpp_strerror(r) << std::endl;
    return -r;
  }

  return 0;
}

void get_check_dirty_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
  options->add_options()
    ("block-on-clean", po::bool_switch(), "block dirty bit updates if the dirty bit is clean");
  options->add_options()
    ("unblock", po::bool_switch(), "unblock dirty bit updates");
}

int execute_check_dirty(const po::variables_map &vm,
            const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  std::string image_id;
  bool block_on_clean;
  bool unblock;

  if (vm.count(at::IMAGE_ID)) {
    image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, image_id.empty(),
    utils::SNAPSHOT_PRESENCE_PERMITTED, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  if (!image_id.empty() && !image_name.empty()) {
    std::cerr << "rbd: trying to access image using both name and id. "
              << std::endl;
    return -EINVAL;
  }
  if (!snap_name.empty()) {
    std::cerr << "rbd: snapshot not supported. "
              << std::endl;
    return -EINVAL;
  }

  block_on_clean = vm["block-on-clean"].as<bool>();
  unblock = vm["unblock"].as<bool>();
  if (block_on_clean && unblock) {
    std::cerr << "rbd: both block and unblock actions specified. "
              << std::endl;
    return -EINVAL;
  }

  if (unblock) {

    r = do_unblock_dfork_dirty(pool_name, namespace_name, image_name, image_id);
    if (r < 0) {
      std::cerr << "rbd: failed to unblock dfork dirty: " << cpp_strerror(r) 
                << std::endl;
      return -r;
    }
  }
  else {

    uint8_t dirty;
    r = do_check_dfork_dirty(pool_name, namespace_name, image_name, image_id,
                             &dirty, block_on_clean);
    if (r < 0) {
      std::cerr << "rbd: failed to check dfork dirty: " << cpp_strerror(r) 
                << std::endl;
      return -r;
    }
    std::cout << (int)dirty << std::endl;
  }

  // librbd::Image image;
  // r = do_check_dfork_dirty(&image, pool_name, namespace_name, image_name, image_id,
  //                          block_on_clean);
  // if (r < 0) {
  //   std::cerr << "rbd: failed to check dfork dirty: " << cpp_strerror(r) << std::endl;
  //   return -r;
  // }

  // uint8_t dirty;
  // r = image.dirty(&dirty);
  // if (r < 0) {
  //   return r;
  // }
  // std::cout << (int)dirty << std::endl;

  return 0;
}

Shell::Action action_create(
  {"dfork", "create"}, {"dfork", "add"}, "dfork a disk image.", "",
  &get_create_arguments, &execute_create);
Shell::Action action_remove(
  {"dfork", "remove"}, {"dfork", "rm"}, "remove a dfork-ed image.", "",
  &get_remove_arguments, &execute_remove);
Shell::Action action_check_dirty(
  {"dfork", "dirty"}, {}, "Check the dfork dirty bit of an image", "",
  &get_check_dirty_arguments, &execute_check_dirty);
Shell::Action action_set_dirty(
  {"dfork", "__dirty"}, {}, "Set the dfork dirty bit (for internal use)", "",
  &get_set_dirty_arguments, &execute_set_dirty);

} // namespace dfork
} // namespace action
} // namespace rbd
