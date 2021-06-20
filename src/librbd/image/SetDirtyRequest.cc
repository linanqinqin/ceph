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
#define LNQQ_DOUT_SetDirtyReq_LVL 100
/* end */

namespace librbd {
namespace image {

// using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
SetDirtyRequest<I>::SetDirtyRequest(IoCtx &ioctx, const std::string &image_id, 
                                    uint8_t dirty, Context *on_finish)
  : m_image_id(image_id), m_dirty(dirty),
    m_on_finish(on_finish), m_error_result(0) {
  m_io_ctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_io_ctx.cct());

  m_header_obj = util::header_name(m_image_id);
}

template <typename I>
void SetDirtyRequest<I>::send() {
  send_set_dfork_dirty();
}

template <typename I>
void SetDirtyRequest<I>::send_set_dfork_dirty() {
  // CephContext *cct = m_image_ctx->cct;
  ldout(m_cct, LNQQ_DOUT_SetDirtyReq_LVL) << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::set_dfork_dirty(&op, m_dirty);

  using klass = SetDirtyRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_set_dfork_dirty>(this);
  /* linanqinqin */
  // ldout(cct, LNQQ_DOUT_SetDirtyReq_LVL) << __func__ << ": " << m_header_oid << dendl;
  /* end */
  int r = m_io_ctx.aio_operate(m_header_obj, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void SetDirtyRequest<I>::handle_set_dfork_dirty(int r) {
  // CephContext *cct = m_image_ctx->cct;
  // ldout(cct, 10) << __func__ << ": r=" << r << dendl;
  ldout(m_cct, LNQQ_DOUT_SetDirtyReq_LVL) << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "set_dfork_dirty failed: " << cpp_strerror(r)
               << dendl;
    m_error_result = r;
    complete(m_error_result);
  }

  complete(0);
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
