// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "acconfig.h"
#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
/* linanqinqin */
#include "tools/rbd/Utils.h"
#include "common/errno.h"
#include <iostream>
/* end */

#include <boost/program_options.hpp>

#include "include/ceph_assert.h"

namespace rbd {
namespace action {

namespace at = argument_types;
namespace po = boost::program_options;

#define DECLARE_DEVICE_OPERATIONS(ns)                                   \
  namespace ns {                                                        \
  int execute_list(const po::variables_map &vm,                         \
                   const std::vector<std::string> &ceph_global_args);   \
  int execute_map(const po::variables_map &vm,                          \
                  const std::vector<std::string> &ceph_global_args);    \
  int execute_unmap(const po::variables_map &vm,                        \
                    const std::vector<std::string> &ceph_global_args);  \
  }

DECLARE_DEVICE_OPERATIONS(ggate);
DECLARE_DEVICE_OPERATIONS(kernel);
DECLARE_DEVICE_OPERATIONS(nbd);
DECLARE_DEVICE_OPERATIONS(wnbd);

namespace device {

namespace {

struct DeviceOperations {
  int (*execute_list)(const po::variables_map &vm,
                      const std::vector<std::string> &ceph_global_args);
  int (*execute_map)(const po::variables_map &vm,
                     const std::vector<std::string> &ceph_global_args);
  int (*execute_unmap)(const po::variables_map &vm,
                       const std::vector<std::string> &ceph_global_args);
};

const DeviceOperations ggate_operations = {
  ggate::execute_list,
  ggate::execute_map,
  ggate::execute_unmap,
};

const DeviceOperations krbd_operations = {
  kernel::execute_list,
  kernel::execute_map,
  kernel::execute_unmap,
};

const DeviceOperations nbd_operations = {
  nbd::execute_list,
  nbd::execute_map,
  nbd::execute_unmap,
};

const DeviceOperations wnbd_operations = {
  wnbd::execute_list,
  wnbd::execute_map,
  wnbd::execute_unmap,
};

enum device_type_t {
  DEVICE_TYPE_GGATE,
  DEVICE_TYPE_KRBD,
  DEVICE_TYPE_NBD,
  DEVICE_TYPE_WNBD,
};

struct DeviceType {};

void validate(boost::any& v, const std::vector<std::string>& values,
              DeviceType *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  #ifdef _WIN32
  if (s == "wnbd") {
    v = boost::any(DEVICE_TYPE_WNBD);
  #else
  if (s == "nbd") {
     v = boost::any(DEVICE_TYPE_NBD);
  } else if (s == "ggate") {
    v = boost::any(DEVICE_TYPE_GGATE);
  } else if (s == "krbd") {
    v = boost::any(DEVICE_TYPE_KRBD);
  #endif /* _WIN32 */
  } else {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
}

void add_device_type_option(po::options_description *options) {
  options->add_options()
    ("device-type,t", po::value<DeviceType>(),
#ifdef _WIN32
     "device type [wnbd]");
#else
     "device type [ggate, krbd (default), nbd]");
#endif
}

void add_device_specific_options(po::options_description *options) {
  options->add_options()
    ("options,o", po::value<std::vector<std::string>>(),
     "device specific options");
}

device_type_t get_device_type(const po::variables_map &vm) {
  if (vm.count("device-type")) {
    return vm["device-type"].as<device_type_t>();
  }
  #ifndef _WIN32
  return DEVICE_TYPE_KRBD;
  #else
  return DEVICE_TYPE_WNBD;
  #endif
}

const DeviceOperations *get_device_operations(const po::variables_map &vm) {
  switch (get_device_type(vm)) {
  case DEVICE_TYPE_GGATE:
    return &ggate_operations;
  case DEVICE_TYPE_KRBD:
    return &krbd_operations;
  case DEVICE_TYPE_NBD:
    return &nbd_operations;
  case DEVICE_TYPE_WNBD:
    return &wnbd_operations;
  default:
    ceph_abort();
    return nullptr;
  }
}

} // anonymous namespace

/* linanqinqin */
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
                           bool do_all, 
                           bool is_child) {
  librados::Rados rados; 
  librados::IoCtx io_ctx;
  int r = utils::init(pool_name, namespace_name, &rados, &io_ctx);
  if (r < 0) {
    return r;
  }

  librbd::RBD rbd;
  r = rbd.dfork_switch(io_ctx, image_name, image_id, switch_on, do_all, is_child);
  if (r < 0) {
    std::cerr << "rbd: error switching dfork mode: "
              << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}
/* end */

void get_list_arguments(po::options_description *positional,
                        po::options_description *options) {
  add_device_type_option(options);
  at::add_format_options(options);
}

int execute_list(const po::variables_map &vm,
                 const std::vector<std::string> &ceph_global_init_args) {
  return (*get_device_operations(vm)->execute_list)(vm, ceph_global_init_args);
}

void get_map_arguments(po::options_description *positional,
                       po::options_description *options) {
  add_device_type_option(options);
  at::add_image_or_snap_spec_options(positional, options,
                                     at::ARGUMENT_MODIFIER_NONE);
  options->add_options()
    ("read-only", po::bool_switch(), "map read-only")
    ("exclusive", po::bool_switch(), "disable automatic exclusive lock transitions")
    ("quiesce", po::bool_switch(), "use quiesce hooks")
    ("quiesce-hook", po::value<std::string>(), "quiesce hook path");
  add_device_specific_options(options);
}

int execute_map(const po::variables_map &vm,
                const std::vector<std::string> &ceph_global_init_args) {
  /* linanqinqin */
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
                      image_name, image_id, true, false, false);
  if (r < 0) {
    return r;
  }

  /* end */
  return (*get_device_operations(vm)->execute_map)(vm, ceph_global_init_args);
}

/* linanqinqin */
int execute_super(const po::variables_map &vm,
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

  // reset the dirty bit
  r = do_reset_dfork_dirty(pool_name.c_str(), namespace_name.c_str(), 
                           image_name.c_str(), image_id.c_str());
  if (r < 0) {
    return r;
  }
  // turn on dfork mode
  r = do_dfork_switch(pool_name, namespace_name, 
                      image_name, image_id, true, false, true);
  if (r < 0) {
    return r;
  }

  return (*get_device_operations(vm)->execute_map)(vm, ceph_global_init_args);
}
/* end */

void get_unmap_arguments(po::options_description *positional,
                         po::options_description *options) {
  add_device_type_option(options);
  positional->add_options()
    ("image-or-snap-or-device-spec",
     "image, snapshot, or device specification\n"
     "[<pool-name>/]<image-name>[@<snap-name>] or <device-path>");
  at::add_pool_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_image_option(options, at::ARGUMENT_MODIFIER_NONE);
  at::add_snap_option(options, at::ARGUMENT_MODIFIER_NONE);
  add_device_specific_options(options);
}

int execute_unmap(const po::variables_map &vm,
                  const std::vector<std::string> &ceph_global_init_args) {
  return (*get_device_operations(vm)->execute_unmap)(vm, ceph_global_init_args);
}

Shell::SwitchArguments switched_arguments({"read-only", "exclusive"});
Shell::Action action_list(
  {"device", "list"}, {"showmapped"}, "List mapped rbd images.", "",
  &get_list_arguments, &execute_list);
// yet another alias for list command
Shell::Action action_ls(
  {"device", "ls"}, {}, "List mapped rbd images.", "",
  &get_list_arguments, &execute_list, false);

Shell::Action action_map(
  {"device", "map"}, {"map"}, "Map an image to a block device.", "",
  &get_map_arguments, &execute_map);

/* linanqinqin */
Shell::Action action_super(
  {"dfork", "super"}, {"super"}, "Create a disk superposition and map the image to a block device", "",
  &get_map_arguments, &execute_super);
/* end */

Shell::Action action_unmap(
  {"device", "unmap"}, {"unmap"}, "Unmap a rbd device.", "",
  &get_unmap_arguments, &execute_unmap);

} // namespace device
} // namespace action
} // namespace rbd
