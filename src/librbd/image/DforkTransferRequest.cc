// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/DforkTransferRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"
#include "librbd/ObjectMap.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::DforkTransferRequest: "

/* linanqinqin */
#define LNQQ_DOUT_DforkTransferReq_LVL 20
/* end */

namespace librbd {
namespace image {

// using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
DforkTransferRequest<I>::DforkTransferRequest(IoCtx &ioctx, 
                                              const std::string &image_name, 
                                              const std::string &image_id, 
                                              Context *on_finish)
  : m_image_name(image_name), 
    m_image_id(image_id), 
    m_on_finish(on_finish), 
    m_error_result(0) {

  m_io_ctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_io_ctx.cct());
}

template <typename I>
void DforkTransferRequest<I>::send() {
  if (!m_image_id.empty()) {
    send_dfork_transfer();
  }
  else if (!m_image_name.empty()) {
    send_get_id();
  }
  else {
    complete(-EINVAL);
  }
}

template <typename I>
void DforkTransferRequest<I>::send_get_id() {

  librados::ObjectReadOperation op;
  cls_client::get_id_start(&op);

  using klass = DforkTransferRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_id>(this);
  
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(util::id_obj_name(m_image_name), 
                               comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void DforkTransferRequest<I>::handle_get_id(int r) {

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
    send_dfork_transfer();
  }
}

template <typename I>
void DforkTransferRequest<I>::send_dfork_transfer() {
  ldout(m_cct, LNQQ_DOUT_DforkTransferReq_LVL) << __func__ << dendl;

  // librados::ObjectWriteOperation op;
  // cls_client::dfork_switch(&op, m_switch_on, m_do_all, m_is_child);

  // using klass = DforkTransferRequest<I>;
  // librados::AioCompletion *comp =
  //   create_rados_callback<klass, &klass::handle_dfork_transfer>(this);

  // m_header_obj = util::header_name(m_image_id);
  // // m_out_bl.clear();
  // int r = m_io_ctx.aio_operate(m_header_obj, comp, &op);
  // ceph_assert(r == 0);
  // comp->release();

  int r = cls_client::object_map_load(&(m_io_ctx), 
                                      "rbd_object_map."+m_image_id, 
                                      &m_object_map);
  if (r) {
    complete(r);
    return;
  }

  const size_t len = 1;
  const uint64_t off = 0;
  bufferlist write_bl;
  bufferptr bp(len);
  memset(bp.c_str(), rand() & 0xff, len);
  write_bl.push_back(bp);

  // std::cout << "linanqinqin object_map.size=" << m_object_map.size() << std::endl;
  auto it = m_object_map.begin();
  auto end_it = m_object_map.end();
  uint64_t pos = 0;
  uint64_t omap_size = m_object_map.size();
  for (; it!=end_it; ++it, ++pos) {
    if (*it != OBJECT_NONEXISTENT) {

      // get the data object name
      char buf[256];
      sprintf(buf, "rbd_data.%s.%016lx", m_image_id.c_str(), pos);
      string data_oid(buf);

      // issue the transfer delete (delete the parent)
      r = m_io_ctx.remove(data_oid, CEPH_OSD_FLAG_DFORK_REMOVE);
      if (r && r!=-ENOENT) {
        lderr(m_cct) << "transfer delete failed on object "
                     << data_oid << ": " << cpp_strerror(r)
                     << dendl;
        complete(r);
        return;
      }

      // issue the transfer write
      r = m_io_ctx.write(data_oid, write_bl, len, off);

      if (pos % 1000 == 0) {
        std::cout << "                                         "
                  << "                                         \r";
        std::cout << "rbd collapse transferring child objects: " 
                  << ((pos+1)*100)/omap_size
                  << "%% complete...\r";
        std::cout.flush();
      }
    }
  }

  std::cout << "                                         "
            << "                                         \r";
  std::cout << "rbd collapse transferring child objects: " 
            << "100%% complete..." 
            << std::endl;
  std::cout.flush();

  complete(0);
}

template <typename I>
void DforkTransferRequest<I>::handle_dfork_transfer(int r) {
  ldout(m_cct, LNQQ_DOUT_DforkTransferReq_LVL) << __func__ << ": r=" << r << dendl;

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
void DforkTransferRequest<I>::complete(int r) {

  auto on_finish = m_on_finish;
  delete this;
  on_finish->complete(r);
}

} // namespace image
} // namespace librbd

template class librbd::image::DforkTransferRequest<librbd::ImageCtx>;
