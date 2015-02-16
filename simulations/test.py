__author__ = 'jingli'


def reissue_ctrl():
    # if self.timeout is not None:
    reissue = None
    try:
        reissue = "abc"
        print("reissue finished first.")
        raise Exception("blabla")
    except Exception as e:
        print("exception" + reissue)
        # print('at ', env.now, i.cause)
        # # if reissue is not None:
        # #     if not reissue.processed:
        # #         reissue.interrupt('interrupt')


reissue_ctrl()