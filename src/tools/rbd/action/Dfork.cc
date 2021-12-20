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

static int do_set_dfork_dirty(const std::string &pool_name, 
                              const std::string &namespace_name, 
                              const std::string &image_name,
                              const std::string &image_id,
                              uint8_t dirty, 
                              const std::string &location_oid) {
  librados::Rados rados; 
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.set_dfork_dirty(io_ctx, image_name, image_id, 
                          dirty, location_oid);
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
                                bool block_on_clean, 
                                bool no_cache) {
  librados::Rados rados; 
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.check_dfork_dirty(io_ctx, image_name, image_id, 
                            dirty, block_on_clean, no_cache);
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
  r = rbd.unblock_dfork_dirty(io_ctx, image_name, image_id);
  if (r < 0) {
    std::cerr << "rbd: error unblocking dfork dirty updates: "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

static int do_reset_dfork_dirty(const std::string &pool_name, 
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
  r = rbd.reset_dfork_dirty(io_ctx, image_name, image_id);
  if (r < 0) {
    std::cerr << "rbd: error resetting dfork dirty: "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

static int do_dfork_switch(const std::string &pool_name, 
                           const std::string &namespace_name,  
                           const std::string &image_name, 
                           const std::string &image_id, 
                           bool switch_on, 
                           bool do_all) {
  librados::Rados rados; 
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.dfork_switch(io_ctx, image_name, image_id, switch_on, do_all);
  if (r < 0) {
    std::cerr << "rbd: error switching dfork mode: "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

static int do_dfork_delete(const std::string &pool_name, 
                           const std::string &namespace_name,  
                           const std::string &image_name, 
                           bool no_progress) {
  librados::Rados rados; 
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  utils::ProgressContext pc("Removing dfork image", no_progress);
  r = rbd.dfork_remove_with_progress(io_ctx, image_name, pc);
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
    r = do_reset_dfork_dirty(pool_name.c_str(), namespace_name.c_str(), 
                             image_name.c_str(), image_id.c_str());
    if (r < 0) {
      std::cerr << "rbd: reset dfork dirty error: " << cpp_strerror(r) << std::endl;
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

int execute_create_v2(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  // std::string image_id;
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
  // uint8_t dirty;
  // r = image.dirty(&dirty);
  // if (r < 0)
  //   return r;
  // if (dirty) {
  //   r = do_reset_dfork_dirty(pool_name.c_str(), namespace_name.c_str(), 
  //                            image_name.c_str(), image_id.c_str());
  //   if (r < 0) {
  //     std::cerr << "rbd: reset dfork dirty error: " << cpp_strerror(r) << std::endl;
  //     return -r;
  //   }
  // }

  /* create the snapshot */
  snap_name = image_name + SNAP_SUFFIX;
  flags |= RBD_SNAP_CREATE_FOR_DFORK;
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
  options->add_options()
    ("reset", po::bool_switch(), "reset the dirty bit");

  options->add_options()
    ("loc-oid", po::value<std::string>(), "the oid associated with the sender");
}

int execute_set_dirty(const po::variables_map &vm,
                   const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  std::string image_id;
  bool is_set, is_clear, is_reset;

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
  is_reset = vm["reset"].as<bool>();
  if (!is_set && !is_clear && !is_reset) {
    std::cerr << "rbd: no set/clear/reset action specified" << std::endl;
    return -EINVAL;
  }
  if (is_set+is_clear+is_reset > 1) {
    std::cerr << "rbd: more than one action (set/clear/reset) specified" << std::endl;
    return -EINVAL;
  }

  if (is_reset) {
    r = do_reset_dfork_dirty(pool_name, namespace_name, 
                             image_name, image_id);
  }
  else {
    std::string location_oid;
    if (vm.count("loc-oid")) {
      location_oid = vm["loc-oid"].as<std::string>();
    }

    r = do_set_dfork_dirty(pool_name, namespace_name, 
                           image_name, image_id, 
                           is_set?1:0, location_oid);
  }

  return r;
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
  options->add_options()
    ("no-cache", po::bool_switch(), "bypass dirty bit cache");
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
  bool no_cache;

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
    no_cache = vm["no-cache"].as<bool>();
    r = do_check_dfork_dirty(pool_name, namespace_name, image_name, image_id,
                             &dirty, block_on_clean, no_cache);
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

void get_dfork_switch_arguments(po::options_description *positional,
                                po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
  options->add_options()
    ("on", po::bool_switch(), "switch on the dfork mode");
  options->add_options()
    ("off", po::bool_switch(), "switch off the dfork mode");
  options->add_options()
    ("all", po::bool_switch(), "targeting all images");
}

int execute_dfork_switch(const po::variables_map &vm,
                         const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  std::string image_id;
  bool is_on, is_off, do_all;
  int r;
  
  do_all = vm["all"].as<bool>();

  if (!do_all) {
    if (vm.count(at::IMAGE_ID)) {
      image_id = vm[at::IMAGE_ID].as<std::string>();
    }

    r = utils::get_pool_image_snapshot_names(
      vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
      &image_name, &snap_name, image_id.empty(),
      utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
    if (r < 0) {
      return r;
    }

    if (!image_id.empty() && !image_name.empty()) {
      std::cerr << "rbd: trying to access image using both name and id. "
                << std::endl;
      return -EINVAL;
    }
    if (!snap_name.empty()) {
      std::cerr << "rbd: operation not supported on snapshots. "
                << std::endl;
      return -EINVAL;
    }
  }
  else {
    image_id = "default";
  }

  is_on = vm["on"].as<bool>();
  is_off = vm["off"].as<bool>();
  if (is_on && is_off) {
    std::cerr << "rbd: please specify either on/off" 
              << std::endl;
    return -EINVAL;
  }
  else if (!is_on && !is_off) {
    std::cerr << "rbd: no on/off action specified" 
              << std::endl;
    return -EINVAL;
  }
  if (is_on && do_all) {
    std::cerr << "rbd: switching on dfork mode for all images not supported" 
              << std::endl;
    return -EOPNOTSUPP;
  }

  r = do_dfork_switch(pool_name, namespace_name, 
                      image_name, image_id, is_on, do_all);

  return r;
}

void get_dfork_abort_arguments(po::options_description *positional,
                                po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_id_option(options);
  at::add_no_progress_option(options);
}

int execute_dfork_abort(const po::variables_map &vm,
                        const std::vector<std::string> &ceph_global_init_args) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string namespace_name;
  std::string image_name;
  std::string snap_name;
  std::string image_id;
  int r;

  if (vm.count(at::IMAGE_ID)) {
    image_id = vm[at::IMAGE_ID].as<std::string>();
  }

  r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &namespace_name,
    &image_name, &snap_name, image_id.empty(),
    utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  if (!image_id.empty() && !image_name.empty()) {
    std::cerr << "rbd: trying to access image using both name and id. "
              << std::endl;
    return -EINVAL;
  }
  if (!snap_name.empty()) {
    std::cerr << "rbd: operation not supported on snapshots. "
              << std::endl;
    return -EINVAL;
  }

  r = do_dfork_switch(pool_name, namespace_name, 
                      image_name, image_id, false, false);
  
  r = do_dfork_delete(pool_name, namespace_name, 
                      image_name, vm[at::NO_PROGRESS].as<bool>());

  return r;
}

Shell::Action action_create(
  {"dfork", "create"}, {"dfork", "add"}, "dfork a disk image.", "",
  &get_create_arguments, &execute_create_v2);
Shell::Action action_remove(
  {"dfork", "remove"}, {"dfork", "rm"}, "remove a dfork-ed image.", "",
  &get_remove_arguments, &execute_remove);
Shell::Action action_check_dirty(
  {"dfork", "dirty"}, {}, "Check the dfork dirty bit of an image", "",
  &get_check_dirty_arguments, &execute_check_dirty);
Shell::Action action_switch(
  {"dfork", "switch"}, {}, "Switch the dfork mode on/off for an image", "",
  &get_dfork_switch_arguments, &execute_dfork_switch);
Shell::Action action_abort(
  {"dfork", "abort"}, {}, "Abort a dfork image", "",
  &get_dfork_abort_arguments, &execute_dfork_abort);
// Below is still with v2 dirty bit, no longer needed
// Shell::Action action_set_dirty(
//   {"dfork", "__dirty"}, {}, "Set the dfork dirty bit (for internal use)", "",
//   &get_set_dirty_arguments, &execute_set_dirty);

} // namespace dfork
} // namespace action
} // namespace rbd
