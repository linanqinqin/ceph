// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_UNBLOCK_DIRTY_REQUEST_H
#define CEPH_LIBRBD_IMAGE_UNBLOCK_DIRTY_REQUEST_H

#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include <string>

class Context;

using librados::IoCtx;

namespace librbd {

namespace image {

template <typename ImageCtxT = ImageCtx>
class UnblockDirtyRequest {
public:
  static UnblockDirtyRequest *create(IoCtx &ioctx, const std::string &image_name, 
                                 const std::string &image_id, 
                                 Context *on_finish) {
    return new UnblockDirtyRequest(ioctx, image_name, image_id, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * @endverbatim
   */

  UnblockDirtyRequest(IoCtx &ioctx, const std::string &image_name, 
                  const std::string &image_id, Context *on_finish);

  IoCtx m_io_ctx;

  std::string m_image_name;
  std::string m_image_id;

  Context *m_on_finish;

  std::string m_header_obj;
  bufferlist m_out_bl;
  
  CephContext *m_cct;
  int m_error_result;

  void send_get_id();
  void handle_get_id(int r);

  void send_unblock_dfork_dirty();
  void handle_unblock_dfork_dirty(int r);

  void complete(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::UnblockDirtyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_UNBLOCK_DIRTY_REQUEST_H
