// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_DFORK_SWITCH_REQUEST_H
#define CEPH_LIBRBD_IMAGE_DFORK_SWITCH_REQUEST_H

#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include <string>

class Context;

using librados::IoCtx;

namespace librbd {

// class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class DforkSwitchRequest {
public:
  static DforkSwitchRequest *create(IoCtx &ioctx, 
                                    const std::string &image_name, 
                                    const std::string &image_id, 
                                    bool switch_on, 
                                    bool do_all, 
                                    bool is_child, 
                                    Context *on_finish) {
    return new DforkSwitchRequest(ioctx, image_name, image_id, 
                                  switch_on, do_all, is_child, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * @endverbatim
   */

  DforkSwitchRequest(IoCtx &ioctx, 
                     const std::string &image_name, 
                     const std::string &image_id, 
                     bool switch_on,  
                     bool do_all, 
                     bool is_child, 
                     Context *on_finish);

  // ImageCtxT *m_image_ctx;
  IoCtx m_io_ctx;

  std::string m_image_name;
  std::string m_image_id;

  bool m_switch_on;
  bool m_do_all;
  bool m_is_child; 

  Context *m_on_finish;

  std::string m_header_obj;
  bufferlist m_out_bl;
  
  CephContext *m_cct;
  int m_error_result;

  void send_get_id();
  void handle_get_id(int r);

  void send_dfork_switch();
  void handle_dfork_switch(int r);

  void complete(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::DforkSwitchRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_DFORK_SWITCH_REQUEST_H
