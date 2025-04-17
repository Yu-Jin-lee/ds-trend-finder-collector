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
            return TXTFileHandler("./lang/ja/kanji.txt").read_lines()
        return TXTFileHandler(f"./lang/ja/kanji_top_{topn}.txt").read_lines()[:topn]
    
    def get_kanji_trend(self) -> list:
        return TXTFileHandler("./lang/ja/kanji_trend.txt").read_lines()
    
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

hiragana = [
    'あ', 'い', 'う', 'え', 'お',
    'か', 'き', 'く', 'け', 'こ',
    'が', 'ぎ', 'ぐ', 'げ', 'ご',
    'さ', 'し', 'す', 'せ', 'そ',
    'ざ', 'じ', 'ず', 'ぜ', 'ぞ',
    'た', 'ち', 'つ', 'て', 'と',
    'だ', 'ぢ', 'づ', 'で', 'ど',
    'な', 'に', 'ぬ', 'ね', 'の',
    'は', 'ひ', 'ふ', 'へ', 'ほ',
    'ば', 'び', 'ぶ', 'べ', 'ぼ',
    'ぱ', 'ぴ', 'ぷ', 'ぺ', 'ぽ',
    'ま', 'み', 'む', 'め', 'も',
    'や',      'ゆ',       'よ',
    'ら', 'り', 'る', 'れ', 'ろ',
    'わ',                 'を',
    'ん'
]

katakana = [
    'ア', 'イ', 'ウ', 'エ', 'オ',
    'カ', 'キ', 'ク', 'ケ', 'コ',
    'ガ', 'ギ', 'グ', 'ゲ', 'ゴ',
    'サ', 'シ', 'ス', 'セ', 'ソ',
    'ザ', 'ジ', 'ズ', 'ゼ', 'ゾ',
    'タ', 'チ', 'ツ', 'テ', 'ト',
    'ダ', 'ヂ', 'ヅ', 'デ', 'ド',
    'ナ', 'ニ', 'ヌ', 'ネ', 'ノ',
    'ハ', 'ヒ', 'フ', 'ヘ', 'ホ',
    'バ', 'ビ', 'ブ', 'ベ', 'ボ',
    'パ', 'ピ', 'プ', 'ペ', 'ポ',
    'マ', 'ミ', 'ム', 'メ', 'モ',
    'ヤ',      'ユ',       'ヨ',
    'ラ', 'リ', 'ル', 'レ', 'ロ',
    'ワ',                 'ヲ',
    'ン'
]

katakana_chouon = [
    'アー', 'イー', 'ウー', 'エー', 'オー',
    'カー', 'キー', 'クー', 'ケー', 'コー',
    'ガー', 'ギー', 'グー', 'ゲー', 'ゴー',
    'サー', 'シー', 'スー', 'セー', 'ソー',
    'ザー', 'ジー', 'ズー', 'ゼー', 'ゾー',
    'ター', 'チー', 'ツー', 'テー', 'トー',
    'ダー', 'ヂー', 'ヅー', 'デー', 'ドー',
    'ナー', 'ニー', 'ヌー', 'ネー', 'ノー',
    'ハー', 'ヒー', 'フー', 'ヘー', 'ホー',
    'バー', 'ビー', 'ブー', 'ベー', 'ボー',
    'パー', 'ピー', 'プー', 'ペー', 'ポー',
    'マー', 'ミー', 'ムー', 'メー', 'モー',
    'ヤー',       'ユー',         'ヨー',
    'ラー', 'リー', 'ルー', 'レー', 'ロー',
    'ワー'
]

youon = [
    'きゃ', 'きゅ', 'きょ',
    'ぎゃ', 'ぎゅ', 'ぎょ',
    'しゃ', 'しゅ', 'しょ',
    'じゃ', 'じゅ', 'じょ',
    'ちゃ', 'ちゅ', 'ちょ',
    'にゃ', 'にゅ', 'にょ',
    'ひゃ', 'ひゅ', 'ひょ',
    'びゃ', 'びゅ', 'びょ',
    'ぴゃ', 'ぴゅ', 'ぴょ',
    'みゃ', 'みゅ', 'みょ',
    'りゃ', 'りゅ', 'りょ',
    
    'キャ', 'キュ', 'キョ',
    'ギャ', 'ギュ', 'ギョ',
    'シャ', 'シュ', 'ショ',
    'ジャ', 'ジュ', 'ジョ',
    'チャ', 'チュ', 'チョ',
    'ニャ', 'ニュ', 'ニョ',
    'ヒャ', 'ヒュ', 'ヒョ',
    'ビャ', 'ビュ', 'ビョ',
    'ピャ', 'ピュ', 'ピョ',
    'ミャ', 'ミュ', 'ミョ',
    'リャ', 'リュ', 'リョ'
]

sokuon_hiragana = [
    'あっ', 'いっ', 'うっ', 'えっ', 'おっ',
    'かっ', 'きっ', 'くっ', 'けっ', 'こっ',
    'がっ', 'ぎっ', 'ぐっ', 'げっ', 'ごっ',
    'さっ', 'しっ', 'すっ', 'せっ', 'そっ',
    'ざっ', 'じっ', 'ずっ', 'ぜっ', 'ぞっ',
    'たっ', 'ちっ', 'つっ', 'てっ', 'とっ',
    'だっ', 'ぢっ', 'づっ', 'でっ', 'どっ',
    'なっ', 'にっ', 'ぬっ', 'ねっ', 'のっ',
    'はっ', 'ひっ', 'ふっ', 'へっ', 'ほっ',
    'ばっ', 'びっ', 'ぶっ', 'べっ', 'ぼっ',
    'ぱっ', 'ぴっ', 'ぷっ', 'ぺっ', 'ぽっ',
    'まっ', 'みっ', 'むっ', 'めっ', 'もっ',
    'やっ',        'ゆっ',        'よっ',
    'らっ', 'りっ', 'るっ', 'れっ', 'ろっ',
    'わっ'
]

sokuon_katakana = [
    'アッ', 'イッ', 'ウッ', 'エッ', 'オッ',
    'カッ', 'キッ', 'クッ', 'ケッ', 'コッ',
    'ガッ', 'ギッ', 'グッ', 'ゲッ', 'ゴッ',
    'サッ', 'シッ', 'スッ', 'セッ', 'ソッ',
    'ザッ', 'ジッ', 'ズッ', 'ゼッ', 'ゾッ',
    'タッ', 'チッ', 'ツッ', 'テッ', 'トッ',
    'ダッ', 'ヂッ', 'ヅッ', 'デッ', 'ドッ',
    'ナッ', 'ニッ', 'ヌッ', 'ネッ', 'ノッ',
    'ハッ', 'ヒッ', 'フッ', 'ヘッ', 'ホッ',
    'バッ', 'ビッ', 'ブッ', 'ベッ', 'ボッ',
    'パッ', 'ピッ', 'プッ', 'ペッ', 'ポッ',
    'マッ', 'ミッ', 'ムッ', 'メッ', 'モッ',
    'ヤッ',        'ユッ',        'ヨッ',
    'ラッ', 'リッ', 'ルッ', 'レッ', 'ロッ',
    'ワッ'
]

gairaigo_katakana = [
    'ヴァ', 'ヴィ', 'ヴェ', 'ヴォ',        # v
    'ファ', 'フィ', 'フェ', 'フォ',        # f
    'ティ', 'トゥ',                      # ti, tu
    'ディ', 'ドゥ', 'デュ',               # di, du
    'ズィ',                             # z
    'ウィ', 'ウェ', 'ウォ',              # w
    'ジェ',                             # j
    'チェ',                             # ch
    'ツィ'                              # ts
]

alphabet = list(string.ascii_lowercase)

number = [str(n) for n in list(range(0,10))]

kanji = Ja().get_kanji(300)

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
    