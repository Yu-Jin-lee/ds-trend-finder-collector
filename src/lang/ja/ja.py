import string
from lang.lang_base import LanguageBase
from utils.file import TXTFileHandler

class Ja(LanguageBase):
    def __init__(self) -> None:
        super().__init__()
        self.alphabets = self.get_alphabets()
        self.numbers = self.get_numbers()
        self.letters = self.get_letters()
    
    def language(self) -> str: 
        return 'ja'

    @property
    def hl(self) -> str:
        return 'ja'

    @property
    def gl(self) -> str:
        return 'jp'
    
    def serp_location(self) -> str:
        return 'Japan'
    
    def get_none(self) -> list:
        return [""]
    
    def get_characters(self) -> list:
        pass
    
    def get_kanji(self, topn:int = None) -> list:
        if topn == None:
            return TXTFileHandler("/data2/yj.lee/git/suggest/src/lang/ja/kanji.txt").read_lines()
        return TXTFileHandler(f"/data2/yj.lee/git/suggest/src/lang/ja/kanji_top_{topn}.txt").read_lines()[:topn]
    
    def get_kanji_trend(self) -> list:
        return TXTFileHandler("/data2/yj.lee/git/suggest/src/lang/ja/kanji_trend.txt").read_lines()
    
    def get_letters(self) -> list:
        return ['あ', 'い', 'う', 'え', 'お',
                'か', 'き', 'く', 'け', 'こ',
                'さ', 'し', 'す', 'せ', 'そ',
                'た', 'ち', 'つ', 'て', 'と',
                'な', 'に', 'ぬ', 'ね', 'の',
                'は', 'ひ', 'ふ', 'へ', 'ほ',
                'ま', 'み', 'む', 'め', 'も',
                'や',       'ゆ',       'よ',
                'ら', 'り', 'る', 'れ', 'ろ',
                'わ',                   'を',
                            'ん'
                ]

    def get_alphabets(self) -> list:
        return list(string.ascii_lowercase)

    def get_numbers(self) -> list:
        return [str(n) for n in list(range(0,10))]
    
    def suggest_extension_texts_by_rank(self, rank) -> list:
        if rank == 0:
            return self.get_none()
        
        elif rank == 1:
            return self.get_letters() + self.get_alphabets() + self.get_numbers()
        elif rank == "1_new":
            return self.get_letters() + self.get_alphabets() + self.get_numbers() + self.get_kanji()
        elif rank == "1_kanji_300":
            return self.get_letters() + self.get_alphabets() + self.get_numbers() + self.get_kanji(300)
        
        elif rank == 2:
            targets = self.get_letters() + self.get_alphabets() + self.get_numbers()
            return [x + y \
                    for x in targets \
                    for y in targets
                    ]
        elif rank == "2_new":
            targets_1 = self.get_letters() + self.get_alphabets() + self.get_numbers()
            targets_2 = self.get_letters() + self.get_alphabets() + self.get_numbers() + self.get_kanji_trend()
            return [x + y \
                    for x in targets_1 \
                    for y in targets_2
                    ]
        elif rank == "2_kanji_300":
            targets_1 = self.get_letters() + self.get_alphabets() + self.get_numbers() + self.get_kanji(300)
            targets_2 = self.get_letters() + self.get_alphabets() + self.get_numbers()
            return [x + y \
                    for x in targets_1 \
                    for y in targets_2
                    ]
        
        elif rank == 3:
            targets = self.get_letters() + self.get_alphabets() + self.get_numbers()
            return [x + y + z \
                    for x in targets \
                    for y in targets \
                    for z in targets
                    ]
        elif rank == "3_new":
            targets_1 = self.get_letters() + self.get_alphabets() + self.get_numbers()
            targets_2 = self.get_letters() + self.get_alphabets() + self.get_numbers()
            targets_3 = self.get_letters() + self.get_alphabets() + self.get_numbers() + self.get_kanji_trend()
            return [x + y + z \
                    for x in targets_1 \
                    for y in targets_2 \
                    for z in targets_3
                    ]
        elif rank == "3_kanji_300":
            targets_1 = self.get_letters() + self.get_alphabets() + self.get_numbers() + self.get_kanji(300)
            targets_2 = self.get_letters() + self.get_alphabets() + self.get_numbers()
            targets_3 = self.get_letters() + self.get_alphabets() + self.get_numbers()
            return [x + y + z \
                    for x in targets_1 \
                    for y in targets_2 \
                    for z in targets_3
                    ]
        
    def suggest_extension_texts(self, 
                                stratgy : str = "all",
                                contain_none : bool = False) -> list:
        if stratgy == "all":
            if contain_none:
                return self.get_none() + \
                       self.get_letters() + \
                       self.get_alphabets() + \
                       self.get_numbers()
            else:
                return self.get_letters() + \
                       self.get_alphabets() + \
                       self.get_numbers()

    def is_domestic(self, text:str, ratio = 0.2):
        try:
            text = text.replace(' ','')
            count = 0
            for char in text:
                if ('\u3040' <= char <= '\u30FF') or ('\uFF66' <= char <= '\uFF9F') :       
                    count += 1
            return (count / len(text)) > ratio
        except:
            return False
        
    def interval_map(self):
        i_map = {'d':' 日前', 'h':' 時間前', 'm':' 分前'}
        return i_map
    
if __name__ == "__main__":
    ja = Ja()

    check_list = ja.suggest_extension_texts_by_rank("1_kanji_300") + ja.suggest_extension_texts_by_rank("2_kanji_300") + ja.suggest_extension_texts_by_rank("3_kanji_300")
    print(check_list)
    print(len(check_list))
    print(type(check_list))

    # kanji = ja.get_kanji()
    # print(kanji)
    # print(type(kanji))
    # print(len(kanji))

    # kanji = ja.get_kanji_trend()
    # print(kanji)
    # print(type(kanji))
    # print(len(kanji))

    # print(ja.hl, ja.gl)
    # print(len(ja.suggest_extension_texts_by_rank(1)),
        #   ja.suggest_extension_texts_by_rank(1))
    