// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_DFORK_REMOVE_REQUEST_H
#define CEPH_LIBRBD_IMAGE_DFORK_REMOVE_REQUEST_H

#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
// #include "librbd/image/TypeTraits.h"

#include <list>

class Context;
class SafeTimer;

namespace librbd {

class ProgressContext;

namespace image {

template<typename ImageCtxT = ImageCtx>
class DforkRemoveRequest {
public:
  static DforkRemoveRequest *create(librados::IoCtx &ioctx,
                                    const std::string &image_name,
                               // const std::string &image_id,
                                    ProgressContext &prog_ctx,
                                    Context *on_finish) {
    return new DforkRemoveRequest(ioctx, image_name, prog_ctx, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * 
   * @endverbatim
   */

  DforkRemoveRequest(librados::IoCtx &ioctx, const std::string &image_name,
                // const std::string &image_id, 
                     ProgressContext &prog_ctx, Context *on_finish);

  librados::IoCtx &m_ioctx;
  ImageCtxT *m_image_ctx = nullptr;

  std::string m_image_name;
  std::string m_image_id;

  ProgressContext &m_prog_ctx;
  Context *m_on_finish;

  CephContext *m_cct;

  // std::string m_header_oid;

  int m_ret_val = 0;

  void open_image();
  void handle_open_image(int r);

  void remove_dfork_objects();
  void handle_remove_dfork_objects(int r);

  void send_close_image(int r);
  void handle_send_close_image(int r);

  void finish(int r);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::DforkRemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_DFORK_REMOVE_REQUEST_H
