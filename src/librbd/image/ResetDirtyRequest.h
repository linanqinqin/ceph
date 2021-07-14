// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_RESET_DIRTY_REQUEST_H
#define CEPH_LIBRBD_IMAGE_RESET_DIRTY_REQUEST_H

#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include <string>

class Context;

using librados::IoCtx;

namespace librbd {

namespace image {

template <typename ImageCtxT = ImageCtx>
class ResetDirtyRequest {
public:
  static ResetDirtyRequest *create(IoCtx &ioctx, const std::string &image_name, 
                                   const std::string &image_id, 
                                   Context *on_finish) {
    return new ResetDirtyRequest(ioctx, image_name, image_id, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    | (if id not provided)
   *    |----------\
   *    |          |
   *    |          v    (on error)
   *    |        GET_ID-------------------\
   *    |          |                      |
   *    |<---------/                      |
   *    |                                 |
   *    v  (lock held)         (on error) |
   * GET_DFORK_DIRTY_LOCATIONS------------|
   *    |                 (lock released) |
   *    v              (on error)         |
   * RESET_DFORK_DIRTY--------------\     |
   *    |                           |     |
   *    v                           |     |
   * CLEAR_DFORK_DIRTY_V2_CACHE-----|     |
   *    |                           |     |
   *    v                           |     |
   * CLEAR_DFORK_DIRTY--------------|     |
   *    |                           |     |
   *    v  (lock released)          |     |
   * CLEAR_DFORK_DIRTY_LOCATIONS <--/     |
   *    |                                 |
   *    v                                 |
   * COMPLETE <---------------------------/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ResetDirtyRequest(IoCtx &ioctx, const std::string &image_name, 
                    const std::string &image_id, Context *on_finish);

  IoCtx m_io_ctx;

  std::string m_image_name;
  std::string m_image_id;

  Context *m_on_finish;

  std::string m_header_obj;
  bufferlist m_out_bl;
  std::string m_locations;
  
  std::string m_omap_id;

  CephContext *m_cct;
  int m_error_result;

  void send_get_id();
  void handle_get_id(int r);

  void send_get_dfork_dirty_locations();
  void handle_get_dfork_dirty_locations(int r);

  bool next_location_oid(std::string *oid);

  void send_reset_dfork_dirty();
  void handle_reset_dfork_dirty(int r);

  void send_clear_dfork_dirty_v2_cache();
  void handle_clear_dfork_dirty_v2_cache(int r);

  void send_clear_dfork_dirty();
  void handle_clear_dfork_dirty(int r);

  void send_clear_dfork_dirty_locations();
  void handle_clear_dfork_dirty_locations(int r);

  void complete(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::ResetDirtyRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_RESET_DIRTY_REQUEST_H
