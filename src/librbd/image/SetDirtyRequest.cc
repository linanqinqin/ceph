// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/SetDirtyRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::SetDirtyRequest: "

/* linanqinqin */
#define LNQQ_DOUT_SetDirtyReq_LVL 0
/* end */

namespace librbd {
namespace image {

// using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
SetDirtyRequest<I>::SetDirtyRequest(IoCtx &ioctx, const std::string &image_name, 
                                    const std::string &image_id, 
                                    uint8_t dirty, 
                                    const std::string &location_oid, 
                                    Context *on_finish)
  : m_image_name(image_name), 
    m_image_id(image_id), 
    m_dirty(dirty),
    m_location_oid(location_oid),
    m_on_finish(on_finish), m_error_result(0) {

  m_io_ctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_io_ctx.cct());
}

template <typename I>
void SetDirtyRequest<I>::send() {
  if (!m_image_id.empty()) {
    send_set_dfork_dirty();
  }
  else if (!m_image_name.empty()) {
    send_get_id();
  }
  else {
    complete(-EINVAL);
  }
}

template <typename I>
void SetDirtyRequest<I>::send_get_id() {

  librados::ObjectReadOperation op;
  cls_client::get_id_start(&op);

  using klass = SetDirtyRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_id>(this);
  
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(util::id_obj_name(m_image_name), 
                               comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void SetDirtyRequest<I>::handle_get_id(int r) {

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
    send_set_dfork_dirty();
  }
}

template <typename I>
void SetDirtyRequest<I>::send_set_dfork_dirty() {
  // CephContext *cct = m_image_ctx->cct;
  ldout(m_cct, LNQQ_DOUT_SetDirtyReq_LVL) << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::set_dfork_dirty(&op, m_dirty, m_location_oid);

  using klass = SetDirtyRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_set_dfork_dirty>(this);
  /* linanqinqin */
  // ldout(cct, LNQQ_DOUT_SetDirtyReq_LVL) << __func__ << ": " << m_header_oid << dendl;
  /* end */

  m_header_obj = util::header_name(m_image_id);
  // m_out_bl.clear();
  int r = m_io_ctx.aio_operate(m_header_obj, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void SetDirtyRequest<I>::handle_set_dfork_dirty(int r) {
  // CephContext *cct = m_image_ctx->cct;
  // ldout(cct, 10) << __func__ << ": r=" << r << dendl;
  ldout(m_cct, LNQQ_DOUT_SetDirtyReq_LVL) << __func__ << ": r=" << r << dendl;

  // if (r == 0) {
  //   auto it = m_out_bl.cbegin();
  //   try {
  //     decode(r, it);
  //   } catch (const ceph::buffer::error &err) {
  //     r = -EBADMSG;
  //   }
  // }

  if (r < 0) {
    lderr(m_cct) << "set_dfork_dirty failed: " << cpp_strerror(r)
                 << dendl;
    m_error_result = r;
    complete(m_error_result);
  }
  else {
    complete(0);
  }
}

template <typename I>
void SetDirtyRequest<I>::complete(int r) {

  auto on_finish = m_on_finish;
  delete this;
  on_finish->complete(r);
}

} // namespace image
} // namespace librbd

template class librbd::image::SetDirtyRequest<librbd::ImageCtx>;
