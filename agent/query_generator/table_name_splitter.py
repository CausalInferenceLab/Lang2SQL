import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


from llm_utils.tools import get_info_from_db
from table_info import table_info
from prepositions_conjunctions_list import prepositions_conjunctions_list
table_info = table_info
def table_name_splitter():
    # table_info = get_info_from_db()
    
    split_table_names = []
    
    for table in table_info:
        table_name = table.page_content.split(':')[0].strip()
        split_names = table_name.split('_')
        split_table_names.append(split_names)
        

    max_len = max(len(names) for names in split_table_names)

    result_lists = [[] for _ in range(max_len)]


    for names in split_table_names:
        for idx in range(max_len):
            if idx < len(names):
                if names[idx] not in prepositions_conjunctions_list:
                    result_lists[idx].append(names[idx])
                else:
                    result_lists[idx].append('')  


    ordinal_names = [
    "first", "second", "third", "fourth", "fifth",
    "sixth", "seventh", "eighth", "ninth", "tenth",
    "eleventh", "twelfth", "thirteenth", "fourteenth", "fifteenth"
    ]


    named_lists = {}
    for idx, lst in enumerate(result_lists):
        if idx < len(ordinal_names):
            var_name = f"{ordinal_names[idx]}_list"
        else:
            var_name = f"{idx+1}_list"
        named_lists[var_name] = lst


    unique_named_lists = {}
    for key, value_list in named_lists.items():
        
        unique_list = list(dict.fromkeys(value_list))
        unique_named_lists[key] = unique_list
        
    return unique_named_lists


if __name__ == "__main__":
    unique_named_lists = table_name_splitter()
    # print(result_lists)
    # print(named_lists)
    # print("="*100)
    # print(unique_named_lists)
    # print("="*100)
    