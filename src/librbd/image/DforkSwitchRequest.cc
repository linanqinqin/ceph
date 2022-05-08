// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/DforkSwitchRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::DforkSwitchRequest: "

/* linanqinqin */
#define LNQQ_DOUT_DforkSwitchReq_LVL 20
/* end */

namespace librbd {
namespace image {

// using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
DforkSwitchRequest<I>::DforkSwitchRequest(IoCtx &ioctx, 
                                          const std::string &image_name, 
                                          const std::string &image_id, 
                                          bool switch_on, 
                                          bool do_all, 
                                          bool is_child, 
                                          Context *on_finish)
  : m_image_name(image_name), 
    m_image_id(image_id), 
    m_switch_on(switch_on), 
    m_do_all(do_all), 
    m_is_child(is_child),
    m_on_finish(on_finish), 
    m_error_result(0) {

  m_io_ctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_io_ctx.cct());
}

template <typename I>
void DforkSwitchRequest<I>::send() {
  if (!m_image_id.empty()) {
    send_dfork_switch();
  }
  else if (!m_image_name.empty()) {
    send_get_id();
  }
  else {
    complete(-EINVAL);
  }
}

template <typename I>
void DforkSwitchRequest<I>::send_get_id() {

  librados::ObjectReadOperation op;
  cls_client::get_id_start(&op);

  using klass = DforkSwitchRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_id>(this);
  
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(util::id_obj_name(m_image_name), 
                               comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void DforkSwitchRequest<I>::handle_get_id(int r) {

  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = cls_client::get_id_finish(&it, &m_image_id);
  }

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve image id: " << cpp_strerror(r)
               << dendl;
    complete(r);
  }
  else {
    send_dfork_switch();
  }
}

template <typename I>
void DforkSwitchRequest<I>::send_dfork_switch() {
  ldout(m_cct, LNQQ_DOUT_DforkSwitchReq_LVL) << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::dfork_switch(&op, m_switch_on, m_do_all, m_is_child);

  using klass = DforkSwitchRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_dfork_switch>(this);

  m_header_obj = util::header_name(m_image_id);
  // m_out_bl.clear();
  int r = m_io_ctx.aio_operate(m_header_obj, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void DforkSwitchRequest<I>::handle_dfork_switch(int r) {
  ldout(m_cct, LNQQ_DOUT_DforkSwitchReq_LVL) << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "dfork_switch failed: " << cpp_strerror(r)
                 << dendl;
    m_error_result = r;
    complete(m_error_result);
  }
  else {
    complete(0);
  }
}

template <typename I>
void DforkSwitchRequest<I>::complete(int r) {

  auto on_finish = m_on_finish;
  delete this;
  on_finish->complete(r);
}

} // namespace image
} // namespace librbd

template class librbd::image::DforkSwitchRequest<librbd::ImageCtx>;
