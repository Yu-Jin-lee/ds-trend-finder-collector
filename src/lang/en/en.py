alphabet_list = [chr(i) for i in range(ord('a'), ord('z')+1)]


import string
from lang.lang_base import LanguageBase
from utils.text import normalize_spaces
from utils.data import remove_duplicates_preserve_order

class En(LanguageBase):
    def __init__(self) -> None:
        super().__init__()
        self.alphabets = self.get_alphabets()
        self.numbers = self.get_numbers()
        self.letters = self.get_letters()
    
    def language(self) -> str: 
        return 'en'

    @property
    def hl(self) -> str:
        return 'en'

    @property
    def gl(self) -> str:
        return 'us'
    
    def serp_location(self) -> str:
        return 'United States'
    
    def get_none(self) -> list:
        return [""]
    
    def get_characters(self) -> list:
        pass

    def get_letters(self) -> list:
        pass
    
    def get_alphabets(self) -> list:
        return list(string.ascii_lowercase)

    def get_numbers(self) -> list:
        return [str(n) for n in list(range(0,10))]
    
    def suggest_extension_texts_by_rank(self, rank) -> list:
        if rank == 0:
            return self.get_none()
        elif rank == 1:
            return self.get_alphabets() + self.get_numbers()
        elif rank == 2:
            targets = self.get_alphabets() + self.get_numbers() + [' ']
            targets = [normalize_spaces((x + y).lstrip()) \
                    for x in targets \
                    for y in targets
                    ] # 앞 공백 제거, 띄어쓰기 2개 이상 제거
            targets = remove_duplicates_preserve_order(targets)
            targets = [t for t in targets if t != "" and len(t) == 2] # ""가 아니고 2글자인 경우만
            return targets
        elif rank == 3:
            targets = self.get_alphabets() + self.get_numbers() + [' ']
            targets = [normalize_spaces((x + y + z).lstrip()) \
                    for x in targets \
                    for y in targets \
                    for z in targets
                    ] # 앞 공백 제거, 띄어쓰기 2개 이상 제거
            targets = remove_duplicates_preserve_order(targets)
            targets = [t for t in targets if t != "" and len(t) == 3] # ""가 아니고 3글자인 경우만
            return targets
        elif rank == 4:
            targets = self.get_alphabets() + self.get_numbers() + [' ']
            targets = [normalize_spaces((x + y + z + a).lstrip()) \
                    for x in targets \
                    for y in targets \
                    for z in targets \
                    for a in targets
                    ] # 앞 공백 제거, 띄어쓰기 2개 이상 제거
            targets = remove_duplicates_preserve_order(targets)
            targets = [t for t in targets if t != "" and len(t) == 4] # ""가 아니고 4글자인 경우만
            return targets
            
    def suggest_extension_texts(self, 
                                stratgy : str = "all",
                                contain_none : bool = False) -> list:
        if stratgy == "all":
            if contain_none:
                return self.get_none() + \
                       self.get_alphabets() + \
                       self.get_numbers()
            else:
                return self.get_alphabets() + \
                       self.get_numbers()

    def interval_map(self):
        i_map = {'d':' days ago', 'h':' hours ago', 'm':' minutes ago'}
        return i_map
    
if __name__ == "__main__":
    en = En()
    print(en.hl, en.gl)
    print(f"==="*10)

    rank = 1
    print(f"rank {rank}")
    targets = en.suggest_extension_texts_by_rank(rank)
    print(f"{len(targets)}개 ({targets[0]} ~ {targets[-1]})")
    print(f"'{targets[0]}'")
    print(f"'{targets[-1]}'")
    print(f"==="*10)

    rank = 2
    print(f"rank {rank}")
    targets = en.suggest_extension_texts_by_rank(rank)
    print(f"{len(targets)}개 ({targets[0]} ~ {targets[-1]})")
    print(f"'{targets[0]}'")
    print(f"'{targets[-1]}'")
    print(f"==="*10)

    rank = 3
    print(f"rank {rank}")
    targets = en.suggest_extension_texts_by_rank(rank)
    print(f"{len(targets)}개 ({targets[0]} ~ {targets[-1]})")
    print(f"'{targets[0]}'")
    print(f"'{targets[-1]}'")
    print(f"==="*10)

    rank = 4
    print(f"rank {rank}")
    targets = en.suggest_extension_texts_by_rank(rank)
    print(f"{len(targets)}개 ({targets[0]} ~ {targets[-1]})")
    print(f"'{targets[0]}'")
    print(f"'{targets[-1]}'")
    print(f"==="*10)
    