// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/DforkRemoveRequest.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "common/ContextCompletion.h"
#include "common/dout.h"
#include "common/errno.h"
#include "osdc/Striper.h"

#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>
#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::DforkRemoveRequest: "

namespace librbd {
namespace operation {

template <typename I>
class C_RemoveDforkObject : public C_AsyncObjectThrottle<I> {
public:
  C_RemoveDforkObject(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                      uint64_t object_no)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_object_no(object_no)
  {
  }

  int send() override {
    I &image_ctx = this->m_image_ctx;
    ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
    // ceph_assert(image_ctx.exclusive_lock == nullptr ||
    //             image_ctx.exclusive_lock->is_lock_owner());

    {
      std::shared_lock image_locker{image_ctx.image_lock};
      if (image_ctx.object_map != nullptr &&
          !image_ctx.object_map->object_may_exist(m_object_no)) {
        return 1;
      }
    }

    string oid = image_ctx.get_object_name(m_object_no);
    ldout(image_ctx.cct, 10) << "removing dfork " << oid << dendl;
    /* linanqinqin */
    // ldout(image_ctx.cct, 0) << "linanqinqin removing dfork " << oid << dendl;
    /* end */

    librados::AioCompletion *rados_completion =
      util::create_rados_callback(this);
    int r = image_ctx.data_ctx.aio_remove(oid, rados_completion, 
                                          CEPH_OSD_FLAG_DFORK_REMOVE);
    ceph_assert(r == 0);
    rados_completion->release();
    return 0;
  }

private:
  uint64_t m_object_no;
};

template <typename I>
DforkRemoveRequest<I>::DforkRemoveRequest(I &image_ctx, 
                                   // uint64_t original_size, uint64_t new_size,
                                          ProgressContext &prog_ctx,
                                          Context *on_finish)
  : AsyncRequest<I>(image_ctx, on_finish), 
    m_new_size(0),
    m_prog_ctx(prog_ctx)
{
  uint64_t original_size = image_ctx.size;
  uint64_t period = image_ctx.get_stripe_period();
  uint64_t new_num_periods = ((m_new_size + period - 1) / period);
  m_delete_off = std::min(new_num_periods * period, original_size);
  // first object we can delete free and clear
  m_delete_start = new_num_periods * image_ctx.get_stripe_count();
  m_delete_start_min = m_delete_start;
  m_num_objects = Striper::get_num_objects(image_ctx.layout, original_size);

  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " trim dfork image " << original_size << " -> "
		 << m_new_size << " periods " << new_num_periods
                 << " discard to offset " << m_delete_off
                 << " delete objects " << m_delete_start
                 << " to " << m_num_objects << dendl;
}

template <typename I>
bool DforkRemoveRequest<I>::should_complete(int r)
{
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: r=" << r << dendl;
  if (r == -ERESTART) {
    ldout(cct, 5) << "trim operation interrupted" << dendl;
    return true;
  } else if (r < 0) {
    lderr(cct) << "trim encountered an error: " << cpp_strerror(r) << dendl;
    return true;
  }

  std::shared_lock owner_lock{image_ctx.owner_lock};
  switch (m_state) {

  case STATE_REMOVE_DFORK_OBJECTS:
    ldout(cct, 5) << " REMOVE_DFORK_OBJECTS" << dendl;
    send_finish(0);
    break;

  case STATE_FINISHED:
    ldout(cct, 5) << "FINISHED" << dendl;
    return true;

  default:
    lderr(cct) << "invalid state: " << m_state << dendl;
    ceph_abort();
    break;
  }
  return false;
}

template <typename I>
void DforkRemoveRequest<I>::send() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if (!image_ctx.data_ctx.is_valid()) {
    lderr(cct) << "missing data pool" << dendl;
    send_finish(-ENODEV);
    return;
  }

  send_remove_dfork_objects();
}

template <typename I>
void DforkRemoveRequest<I>::send_remove_dfork_objects() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  ldout(image_ctx.cct, 5) << this << " send_remove_dfork_objects: "
			    << " delete_start=" << m_delete_start
			    << " num_objects=" << m_num_objects << dendl;
  m_state = STATE_REMOVE_DFORK_OBJECTS;

  Context *ctx = this->create_callback_context();
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_RemoveDforkObject<I> >(),
      boost::lambda::_1, &image_ctx, boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, image_ctx, context_factory, ctx, &m_prog_ctx, m_delete_start,
    m_num_objects);
  throttle->start_ops(
    image_ctx.config.template get_val<uint64_t>("rbd_concurrent_management_ops"));
}

template <typename I>
void DforkRemoveRequest<I>::send_finish(int r) {
  m_state = STATE_FINISHED;
  this->async_complete(r);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::DforkRemoveRequest<librbd::ImageCtx>;
