// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/ResetDirtyRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::ResetDirtyRequest: "

/* linanqinqin */
#define LNQQ_DOUT_ResetDirtyReq_LVL 10
/* end */

namespace librbd {
namespace image {

using util::create_rados_callback;

template <typename I>
ResetDirtyRequest<I>::ResetDirtyRequest(IoCtx &ioctx, 
                                        const std::string &image_name, 
                                        const std::string &image_id,
                                        Context *on_finish)
  : m_image_name(image_name), 
    m_image_id(image_id), 
    m_on_finish(on_finish), m_error_result(0) {

  m_io_ctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_io_ctx.cct());
}

template <typename I>
void ResetDirtyRequest<I>::send() {
  if (!m_image_id.empty()) {
    send_get_dfork_dirty_locations();
  }
  else if (!m_image_name.empty()) {
    send_get_id();
  }
  else {
    complete(-EINVAL);
  }
}

template <typename I>
void ResetDirtyRequest<I>::send_get_id() {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_id_start(&op);

  using klass = ResetDirtyRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_id>(this);
  
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(util::id_obj_name(m_image_name), 
                               comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ResetDirtyRequest<I>::handle_get_id(int r) {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << dendl;

  /* for testing */
  // r = -EPERM;
  /* end */

  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = cls_client::get_id_finish(&it, &m_image_id);
  }

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve image id: " << cpp_strerror(r)
               << dendl;
    m_error_result = r;
    complete(m_error_result);
  }
  else {
    send_get_dfork_dirty_locations();
  }
}

template <typename I>
void ResetDirtyRequest<I>::send_get_dfork_dirty_locations() {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_dfork_dirty_locations_start(&op);

  using klass = ResetDirtyRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_dfork_dirty_locations>(this);
  
  m_header_obj = util::header_name(m_image_id);

  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(m_header_obj, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ResetDirtyRequest<I>::handle_get_dfork_dirty_locations(int r) {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << ": r=" << r << dendl;

  if (r == 0) {
    auto it = m_out_bl.cbegin();

    r = cls_client::get_dfork_dirty_locations_finish(&it, &m_locations);
    ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << ": " << m_locations << dendl;
  }

  if (r < 0) {
    lderr(m_cct) << "get_dfork_dirty_locations failed: " << cpp_strerror(r)
                 << dendl;
    m_error_result = r;
    complete(m_error_result);
  }
  else {
    send_reset_dfork_dirty();
  }
}

template <typename I>
bool ResetDirtyRequest<I>::next_location_oid(std::string *oid) {

  const char delim = '&';

  static size_t start = 0;
  static size_t end = m_locations.find(delim);

  if (m_locations.empty() || end == 0) {
    return false;
  }

  if (end != std::string::npos) {
    *oid = RBD_DATA_PREFIX + m_image_id + '.' + 
           m_locations.substr(start, end - start);
    start = end + 1;
    end = m_locations.find(delim, start);
    return true;
  }
  else {
    *oid = RBD_DATA_PREFIX + m_image_id + '.' + 
           m_locations.substr(start, end);
    end = 0;
    return true;
  }
}

template <typename I>
void ResetDirtyRequest<I>::send_reset_dfork_dirty() {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << dendl;

  std::string loc_oid;
  if (next_location_oid(&loc_oid)) {
    ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << loc_oid << dendl;

    librados::ObjectWriteOperation op;
    cls_client::reset_dfork_dirty(&op);

    using klass = ResetDirtyRequest<I>;
    librados::AioCompletion *comp =
      create_rados_callback<klass, &klass::handle_reset_dfork_dirty>(this);

    int r = m_io_ctx.aio_operate(loc_oid, comp, &op);
    ceph_assert(r == 0);
    comp->release();
  }
  else {
    send_clear_dfork_dirty();
  }
}

template <typename I>
void ResetDirtyRequest<I>::handle_reset_dfork_dirty(int r) {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << dendl;

  if (r == 0) {
    // move to the next target;
    send_reset_dfork_dirty();
  }
  else {
    lderr(m_cct) << "get_dfork_dirty_locations failed: " << cpp_strerror(r)
                 << dendl;
    m_error_result = r;
    send_clear_dfork_dirty_locations();
  }
}

template <typename I>
void ResetDirtyRequest<I>::send_clear_dfork_dirty() {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::__set_dfork_dirty(&op, 0);

  using klass = ResetDirtyRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_clear_dfork_dirty>(this);

  int r = m_io_ctx.aio_operate(m_header_obj, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ResetDirtyRequest<I>::handle_clear_dfork_dirty(int r) {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << dendl;

  if (r < 0) {
    lderr(m_cct) << "set_dfork_dirty failed: " << cpp_strerror(r)
                 << dendl;
    m_error_result = r;
  }

  send_clear_dfork_dirty_locations();
}

template <typename I>
void ResetDirtyRequest<I>::send_clear_dfork_dirty_locations() {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::clear_dfork_dirty_locations(&op, m_error_result?false:true);

  using klass = ResetDirtyRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_clear_dfork_dirty_locations>(this);

  int r = m_io_ctx.aio_operate(m_header_obj, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ResetDirtyRequest<I>::handle_clear_dfork_dirty_locations(int r) {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << dendl;

  if (r < 0) {
    lderr(m_cct) << "set_dfork_dirty failed: " << cpp_strerror(r)
                 << dendl;
    m_error_result = r;
  }

  complete(m_error_result);
}

template <typename I>
void ResetDirtyRequest<I>::complete(int r) {
  ldout(m_cct, LNQQ_DOUT_ResetDirtyReq_LVL) << __func__ << dendl;

  auto on_finish = m_on_finish;
  delete this;
  on_finish->complete(r);
}

} // namespace image
} // namespace librbd

template class librbd::image::ResetDirtyRequest<librbd::ImageCtx>;
