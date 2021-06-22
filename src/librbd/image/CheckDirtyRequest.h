// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_CHECK_DIRTY_REQUEST_H
#define CEPH_LIBRBD_IMAGE_CHECK_DIRTY_REQUEST_H

#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include <string>

class Context;

using librados::IoCtx;

namespace librbd {

// class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class CheckDirtyRequest {
public:
  static CheckDirtyRequest *create(IoCtx &ioctx, const std::string &image_name,
                                   const std::string &image_id,
                                   uint8_t *dirty, bool block_on_clean, 
                                   Context *on_finish) {
    return new CheckDirtyRequest(ioctx, image_name, image_id, 
                                 dirty, block_on_clean, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * @endverbatim
   */

  CheckDirtyRequest(IoCtx &ioctx, const std::string &image_name,
                    const std::string &image_id, 
                    uint8_t *dirty, bool block_on_clean, 
                    Context *on_finish);

  // ImageCtxT *m_image_ctx;
  IoCtx m_io_ctx;

  std::string m_image_name;
  std::string m_image_id;

  uint8_t *m_dirty;
  bool m_block_on_clean;

  Context *m_on_finish;

  std::string m_header_obj;
  bufferlist m_out_bl;

  CephContext *m_cct;
  int m_error_result;

  void send_get_id();
  void handle_get_id(int r);

  void send_check_dfork_dirty();
  void handle_check_dfork_dirty(int r);

  void complete(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::CheckDirtyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_CHECK_DIRTY_REQUEST_H


// // -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// // vim: ts=8 sw=2 smarttab

// #ifndef CEPH_LIBRBD_IMAGE_CHECK_DIRTY_REQUEST_H
// #define CEPH_LIBRBD_IMAGE_CHECK_DIRTY_REQUEST_H

// #include "include/rados/librados.hpp"
// #include "librbd/ImageCtx.h"
// #include <string>

// class Context;

// using librados::IoCtx;

// namespace librbd {

// class ImageCtx;

// namespace image {

// template <typename ImageCtxT = ImageCtx>
// class CheckDirtyRequest {
// public:
//   static CheckDirtyRequest *create(ImageCtxT *image_ctx, bool block_on_clean, 
//                                    Context *on_finish) {
//     return new CheckDirtyRequest(image_ctx, block_on_clean, on_finish);
//   }

//   void send();

// private:
//   /**
//    * @verbatim
//    *
//    * @endverbatim
//    */

//   CheckDirtyRequest(ImageCtxT *image_ctx, bool block_on_clean, Context *on_finish);

//   ImageCtxT *m_image_ctx;
  
//   bool m_block_on_clean;

//   Context *m_on_finish;
//   CephContext *m_cct;
//   bufferlist m_out_bl;
//   int m_error_result;

//   void send_check_dfork_dirty();
//   void handle_check_dfork_dirty(int r);

//   void complete(int r);

// };

// } // namespace image
// } // namespace librbd

// extern template class librbd::image::CheckDirtyRequest<librbd::ImageCtx>;

// #endif // CEPH_LIBRBD_IMAGE_CHECK_DIRTY_REQUEST_H
