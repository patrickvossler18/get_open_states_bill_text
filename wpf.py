# -*- coding: utf-8 -*-

WPType_None = 0
WPType_Text = 1
WPType_Byte = 2
WPType_Fixed = 3
WPType_Variable = 4

plain_remap = {10:'\n', 11:' ', 13:' ', 160:' ', 169:'-', 170:'-'}

WpCharacterSet = { 0x0121:'à', 0x0406:'§', 0x041c:u'’', 0x041d:u'‘', 0x041f:'”', 0x0420:'“', 0x0422:'—' }

textAttributes = [
    "Extra Large",
    "Very Large",
    "Large",
    "Small",
    "Fine",
    "Superscript",
    "Subscript",
    "Outline",
    "Italic",
    "Shadow",
    "Redline",
    "Double Underline",
    "Bold",
    "Strikeout",
    "Underline",
    "SmallCaps" ]

class WPElem:
    def __init__(self, type=WPType_None, data = [], code=None):
        self.type = type
        self.code = code
        if type == WPType_Text:
            self.data = data
        else:
            self.data = data


class WordPerfect:
    def __init__(self, file):
        self.data = file
        sig = ''.join(chr(x) for x in self.data[1:4])
        if self.data[0] != 255 or sig != 'WPC':
            raise TypeError('Invalid file type')
        self.data_start = self.data[4]+256*(self.data[5]+256*(self.data[6]+256*self.data[7]))
        self.length = len(self.data)
        self.elements = []
        self.parse (self.data_start, self.length)

    def parse (self, start,maxlength):
        pos = start
        while pos < maxlength:
            byte = self.data[pos]
            if byte in plain_remap:
                byte = ord(plain_remap[byte])
            if byte == 10 or byte >= 32 and byte <= 126:
                if len(self.elements) == 0 or self.elements[-1].type != WPType_Text:
                    self.elements.append(WPElem(WPType_Text, ''))
                self.elements[-1].data += chr(byte)
                pos += 1
            elif byte == 12:
                self.elements.append(WPElem(WPType_Text, '\n\n'))
                pos += 1
            elif byte == 0x8c:  # [HRt/Pg Break]
                self.elements.append(WPElem(WPType_Text, '\n'))
                pos += 1
            elif byte == 0x8d:  # [Ftn Num]
                self.elements.append(WPElem(WPType_Text, '[Ftn Num]'))
                pos += 1
            elif byte == 0x99:  # [HRt/Top of Pg]
                self.elements.append(WPElem(WPType_Text, '\n'))
                pos += 1
            elif byte == 0xc0 and pos+3 < maxlength and self.data[pos+3] == 0xc0:
                wpchar = self.data[pos+1]+256*self.data[pos+2]
                if wpchar in WpCharacterSet:
                    self.elements.append(WPElem(WPType_Text, WpCharacterSet[wpchar]))
                else:
                    self.elements.append(WPElem(WPType_Text, '{CHAR:%04X}' % wpchar))
                pos += 4
            elif byte == 0xc1 and self.data[pos+8] == 0xc1:
                # self.elements.append(WPElem(WPType_Fixed, self.data[pos:pos+7]))
                self.elements.append(WPElem(WPType_Text, '\t'))
                pos += 9
            elif byte == 0xc2 and self.data[pos+10] == 0xc2:
                # self.elements.append(WPElem(WPType_Fixed, self.data[pos:pos+9]))
                self.elements.append(WPElem(WPType_Text, '\t'))
                pos += 11
            elif byte == 0xc3:
                self.elements.append(WPElem(WPType_Fixed, self.data[pos:pos+1], '%s On' % textAttributes[self.data[pos+1]]))
                pos += 3
            elif byte == 0xc4:
                self.elements.append(WPElem(WPType_Fixed, self.data[pos:pos+1], '%s Off' % textAttributes[self.data[pos+1]]))
                pos += 3
            elif byte == 0xc6:
                self.elements.append(WPElem(WPType_Fixed, self.data[pos:pos+5]))
                pos += 6
            elif byte == 0xd6 and self.data[pos+1] == 0:    # Footnote
                self.elements.append(WPElem(WPType_Text, '[Footnote:'))
                length = self.data[pos+2]+256*self.data[pos+3]
                self.parse (pos+0x13, pos+length)
                pos += 4+length
                self.elements.append(WPElem(WPType_Text, ']'))

            else:
                self.elements.append(WPElem(WPType_Byte, [byte]))
                if byte >= 0xd0 and pos+4 <= maxlength:
                    length = self.data[pos+2]+256*self.data[pos+3]
                    if pos+4+length <= self.length:
                        if pos+4+length <= self.length and self.data[pos+4+length-1] == byte:
                            self.elements[-1].type = WPType_Variable
                            self.elements[-1].data += [x for x in self.data[pos+1:pos+length]]
                            pos += 4+length
                        else:
                            pos += 1
                    else:
                        pos += 1
                else:
                    pos += 1


# if len(sys.argv) != 2:
#     print("usage: read_wpf.py [suitably ancient WordPerfect file]")
#     sys.exit(1)
#
# wpdata = WordPerfect (sys.argv[1])
#
# for i in wpdata.elements:
#     if i.type == WPType_Text:
#         print (i.data, end='')
# '''
#     elif i.code:
#         print ('[%s]' % i.code, end='')
#     elif i.type == WPType_Variable:
#         print ('[%02X:%d]' % (i.data[0],i.data[1]), end='')
#     else:
#         print ('[%02X]' % i.data[0], end='')
# '''