// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_OPERATION_DFORK_REMOVE_REQUEST_H
#define CEPH_LIBRBD_OPERATION_DFORK_REMOVE_REQUEST_H

#include "librbd/AsyncRequest.h"

namespace librbd
{

class ImageCtx;
class ProgressContext;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class DforkRemoveRequest : public AsyncRequest<ImageCtxT>
{
public:
  static DforkRemoveRequest *create(ImageCtxT &image_ctx, 
                             // uint64_t original_size, uint64_t new_size,
                                    ProgressContext &prog_ctx,
                                    Context *on_finish) {
    return new DforkRemoveRequest(image_ctx, prog_ctx, on_finish);
  }

  DforkRemoveRequest(ImageCtxT &image_ctx, 
                             // uint64_t original_size, uint64_t new_size,
                     ProgressContext &prog_ctx,
                     Context *on_finish);

  void send() override;

protected:
  /**
   * Trim goes through the following state machine to remove whole objects,
   * clean partially trimmed objects, and update the object map:
   *
   * @verbatim
   *
   * 
   */

  enum State {
    STATE_REMOVE_DFORK_OBJECTS,
    STATE_FINISHED
  };

  bool should_complete(int r) override;

  State m_state = STATE_REMOVE_DFORK_OBJECTS;

private:
  uint64_t m_delete_start;
  uint64_t m_delete_start_min = 0;
  uint64_t m_num_objects;
  uint64_t m_delete_off;
  uint64_t m_new_size;

  ProgressContext &m_prog_ctx;

  void send_remove_dfork_objects();

  void send_clean_boundary();
  void send_finish(int r);
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::DforkRemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_DFORK_REMOVE_REQUEST_H
