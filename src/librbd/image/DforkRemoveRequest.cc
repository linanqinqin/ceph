// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/DforkRemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/internal.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/operation/DforkRemoveRequest.h" 

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::DforkRemoveRequest: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace image {

using librados::IoCtx;
using util::create_context_callback;
using util::create_async_context_callback;
using util::create_rados_callback;

template<typename I>
DforkRemoveRequest<I>::DforkRemoveRequest(IoCtx &ioctx, 
                                          const std::string &image_name,
                                          ProgressContext &prog_ctx,
                                          Context *on_finish)
  : m_ioctx(ioctx), 
    m_image_name(image_name), 
    m_prog_ctx(prog_ctx), 
    m_on_finish(on_finish) {
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
}

template<typename I>
void DforkRemoveRequest<I>::send() {
  ldout(m_cct, 20) << dendl;

  open_image();
}

template<typename I>
void DforkRemoveRequest<I>::open_image() {
  // if (m_image_ctx != nullptr) {
  //   pre_remove_image();
  //   return;
  // }

  m_image_ctx = I::create(m_image_id.empty() ? m_image_name : "", m_image_id,
                          nullptr, m_ioctx, false);

  ldout(m_cct, 20) << dendl;

  using klass = DforkRemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_open_image>(
    this);

  m_image_ctx->state->open(OPEN_FLAG_SKIP_OPEN_PARENT, ctx);
}

template<typename I>
void DforkRemoveRequest<I>::handle_open_image(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    m_image_ctx = nullptr;

    if (r != -ENOENT) {
      lderr(m_cct) << "error opening image: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    // remove_image();
    finish(r);
    return;
  }

  // m_image_id = m_image_ctx->id;
  // m_image_name = m_image_ctx->name;
  // m_header_oid = m_image_ctx->header_oid;
  // m_old_format = m_image_ctx->old_format;
  // m_unknown_format = false;

  // pre_remove_image();
  remove_dfork_objects();
}

template<typename I>
void DforkRemoveRequest<I>::remove_dfork_objects() {
  ldout(m_cct, 20) << dendl;

  using klass = DforkRemoveRequest<I>;
  Context *ctx = create_async_context_callback(
    *m_image_ctx, create_context_callback<
      klass, &klass::handle_remove_dfork_objects>(this));

  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  // auto req = librbd::operation::TrimRequest<I>::create(
  //   *m_image_ctx, ctx, m_image_ctx->size, 0, m_prog_ctx);
  auto req = librbd::operation::DforkRemoveRequest<I>::create(
    *m_image_ctx, m_prog_ctx, ctx);
  req->send();
}

template<typename I>
void DforkRemoveRequest<I>::handle_remove_dfork_objects(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove some dfork object(s): "
                 << cpp_strerror(r) << dendl;
    // send_close_image(r);
    // return;
  }

  // if (m_old_format) {
  //   send_close_image(r);
  //   return;
  // }

  // detach_child();
  send_close_image(r);
}

template<typename I>
void DforkRemoveRequest<I>::send_close_image(int r) {
  ldout(m_cct, 20) << dendl;

  m_ret_val = r;
  using klass = DforkRemoveRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_send_close_image>(this);

  m_image_ctx->state->close(ctx);
}

template<typename I>
void DforkRemoveRequest<I>::handle_send_close_image(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "error encountered while closing image: "
                 << cpp_strerror(r) << dendl;
  }

  m_image_ctx = nullptr;
  if (m_ret_val < 0) {
    r = m_ret_val;
  }

  finish(r);
  return;
}

template<typename I>
void DforkRemoveRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::DforkRemoveRequest<librbd::ImageCtx>;
