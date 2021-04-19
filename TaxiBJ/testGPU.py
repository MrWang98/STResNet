import torch
import os
print(torch.cuda.device_count())
flag = torch.cuda.is_available()
print(flag)