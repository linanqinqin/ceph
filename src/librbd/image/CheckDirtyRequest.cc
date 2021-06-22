// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/CheckDirtyRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::CheckDirtyRequest: "

/* linanqinqin */
#define LNQQ_DOUT_CheckDirtyReq_LVL 0
/* end */

namespace librbd {
namespace image {

// using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
CheckDirtyRequest<I>::CheckDirtyRequest(IoCtx &ioctx, const std::string &image_name, 
                                        const std::string &image_id,
                                        uint8_t *dirty, bool block_on_clean, 
                                        Context *on_finish)
  : m_image_name(image_name), m_image_id(image_id), 
    m_dirty(dirty), 
    m_block_on_clean(block_on_clean),
    m_on_finish(on_finish), m_error_result(0) {

  m_io_ctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_io_ctx.cct());
}

template <typename I>
void CheckDirtyRequest<I>::send() {
  if (!m_image_id.empty()) {
    send_check_dfork_dirty();
  }
  else if (!m_image_name.empty()) {
    send_get_id();
  }
  else {
    complete(-EINVAL);
  }
}

template <typename I>
void CheckDirtyRequest<I>::send_get_id() {

  librados::ObjectReadOperation op;
  cls_client::get_id_start(&op);

  using klass = CheckDirtyRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_id>(this);
  
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(util::id_obj_name(m_image_name), 
                               comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void CheckDirtyRequest<I>::handle_get_id(int r) {

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
    send_check_dfork_dirty();
  }
}

template <typename I>
void CheckDirtyRequest<I>::send_check_dfork_dirty() {
  ldout(m_cct, LNQQ_DOUT_CheckDirtyReq_LVL) << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::check_dfork_dirty_start(&op, m_block_on_clean, m_image_id);

  using klass = CheckDirtyRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_check_dfork_dirty>(this);
  /* linanqinqin */
  // ldout(cct, LNQQ_DOUT_CheckDirtyReq_LVL) << __func__ << ": " << m_header_oid << dendl;
  /* end */
  
  m_header_obj = util::header_name(m_image_id);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(m_header_obj, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void CheckDirtyRequest<I>::handle_check_dfork_dirty(int r) {
  // ldout(cct, 10) << __func__ << ": r=" << r << dendl;
  ldout(m_cct, LNQQ_DOUT_CheckDirtyReq_LVL) << __func__ << ": r=" << r << dendl;

  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = cls_client::check_dfork_dirty_finish(&it, m_dirty);
  }

  if (r < 0) {
    lderr(m_cct) << "set_dfork_dirty failed: " << cpp_strerror(r)
                 << dendl;
    m_error_result = r;
    complete(m_error_result);
  }

  complete(0);
}

template <typename I>
void CheckDirtyRequest<I>::complete(int r) {

  auto on_finish = m_on_finish;
  delete this;
  on_finish->complete(r);
  // ldout(m_cct, LNQQ_DOUT_CheckDirtyReq_LVL) << __func__ << ": r=" << r << dendl;
}

} // namespace image
} // namespace librbd

template class librbd::image::CheckDirtyRequest<librbd::ImageCtx>;
