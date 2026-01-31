import sys

class KGException(Exception):
    def __init__(self, error_message, error_detail: sys):
        _, _, exc_tb = error_detail.exc_info()
        self.file_name = exc_tb.tb_frame.f_code.co_filename
        self.line_no = exc_tb.tb_lineno
        super().__init__(
            f"Error in {self.file_name}, line {self.line_no}: {error_message}"
        )
