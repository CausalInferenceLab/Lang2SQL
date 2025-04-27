import os
from argparse import ArgumentParser

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai.chat_models import ChatOpenAI
from tqdm import tqdm
from utils import load_persona_json, save_question_json


def get_persona_prompt(persona):
    return f"""
    Name: {persona.name}
    Department: {persona.department}
    Role: {persona.role}
    Background: {persona.background}
    """


def split_question(question):
    question = question.content
    # remove -
    question = question.replace("- ", "")
    return question.split("\n")


def gen_question(persona):
    llm = llm = ChatOpenAI(model="gpt-4.1-nano", temperature=0)
    prompt = get_persona_prompt(persona)
    system_prompt = """당신은 <persona> 에 해당하는 사람이며 Text2SQL 서비스를 사용하고 있다. 궁금한 질문들을 아래 <format> 에 해당하는 형식으로 질문하라 질문은 다양하게 생성하라

<persona>
{persona_prompt}
</persona>

<format>
- 질문 1
- 질문 2
- 질문 3
...
- 질문 n
</format>
"""
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
        ]
    )

    chain = prompt | llm
    result = {}

    question = chain.invoke({"persona_prompt": prompt})
    result["questions"] = split_question(question)
    result["questions_md"] = question.content
    result["persona"] = persona
    return result


def main(persona_path, output_dir):
    personas = load_persona_json(persona_path)
    for i, persona in tqdm(enumerate(personas.personas)):
        result = gen_question(persona)
        file_path = os.path.join(output_dir, f"{i}.json")
        save_question_json(result, file_path)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--persona_path", type=str, default="data/personas.json")
    parser.add_argument("--output_dir", type=str, default="data/questions")
    args = parser.parse_args()
    main(args.persona_path, args.output_dir)
