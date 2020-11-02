import torch
from torch.utils.data import Dataset
from emp.ray_dataloader import RayDataLoader


class CustomDataset(Dataset):

    def __init__(self, length, tensor_size):
        super(CustomDataset, self).__init__()
        self.length = length
        self.tensor_size = tensor_size

    def __len__(self):
        return self.length

    def __getitem__(self, index):
        tensor = torch.empty(*self.tensor_size).fill_(index)
        tensor = tensor.cuda()
        for _ in range(100):
            tensor /= 2
            tensor *= 2
        return tensor


def test_dataloader():
    torch.cuda.set_device(3)
    dataset = CustomDataset(100, [5])
    loader = RayDataLoader(dataset, batch_size=2, shuffle=False, num_workers=2)
    for epoch in range(1):
        for batch_index, batch in enumerate(loader):
            assert list(batch.size()) == [2, 5]
            assert batch.device == torch.device('cuda:3')
