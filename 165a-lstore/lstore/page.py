
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < 512 #Max records is 512 (4KB total size with 8 byte entries)

    def write(self, value):
        value_b = value.to_bytes(8,signed = True,byteorder = "little") #convert integer into 64 bit integer
        start = self.num_records*8
        self.data[start:start + 8] = value_b # Fill 8 byte spaces from the bytearray given
        self.num_records += 1

    def read(self, index):
        start = index*8
        value_b = self.data[start:start + 8]
        return int.from_bytes(value_b, byteorder = "little", signed = True)


    def update(self, index, value):
        start = index * 8
        self.data[start:start+8] = int(value).to_bytes(8, "little", signed=True)