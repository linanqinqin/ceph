// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_SET_DIRTY_REQUEST_H
#define CEPH_LIBRBD_IMAGE_SET_DIRTY_REQUEST_H

#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include <string>

class Context;

using librados::IoCtx;

namespace librbd {

// class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class SetDirtyRequest {
public:
  static SetDirtyRequest *create(IoCtx &ioctx, const std::string &image_id, 
                                 uint8_t dirty, Context *on_finish) {
    return new SetDirtyRequest(ioctx, image_id, dirty, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * @endverbatim
   */

  SetDirtyRequest(IoCtx &ioctx, const std::string &image_id, uint8_t dirty, Context *on_finish);

  // ImageCtxT *m_image_ctx;
  IoCtx m_io_ctx;
  std::string m_image_id;
  std::string m_header_obj;
  Context *m_on_finish;

  uint8_t m_dirty;

  CephContext *m_cct;
  int m_error_result;

  void send_set_dfork_dirty();
  void handle_set_dfork_dirty(int r);

  void complete(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::SetDirtyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_SET_DIRTY_REQUEST_H
