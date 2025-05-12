
from utils import save_persona_json, pretty_print_persona
from persona_class import PersonaList

from llm_utils.tools import _get_table_info
from langchain_openai.chat_models import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from argparse import ArgumentParser


def get_table_des_string(tables_desc):
    return_string = "table name : table description\n---\n"
    for table_name, table_desc in tables_desc.items():
        return_string += f"{table_name} : {table_desc}\n---\n"
    return return_string


def generate_persona(tables_desc):
    description_string = get_table_des_string(tables_desc)

    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    system_prompt = """주어진 Tabel description들을 참고하여 Text2SQL 서비스로 질문을 할만한 패르소나를 생성하세요"""

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
        ]
    )

    chain = prompt | llm.with_structured_output(PersonaList)
    return chain.invoke({"input": description_string})


def main(output_path):
    # 데이터허브 서버 연결
    tables_desc = _get_table_info()
    personas = generate_persona(tables_desc)

    for persona in personas.personas:
        print(pretty_print_persona(persona))
    save_persona_json(personas, output_path)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--output_path", type=str, default="data/personas.json")
    args = parser.parse_args()
    main(args.output_path)
