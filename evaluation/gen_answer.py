import uuid
from argparse import ArgumentParser

from langchain_core.messages import HumanMessage
from tqdm import tqdm
from utils import load_question_json, save_answer_json

from llm_utils.graph import builder


def get_eval_result(
    graph,
    name=None,
    version=None,
    desc="",
    debug=False,
    input_dir="data/questions",
    output_dir="data/q_sql",
):

    if name is None:
        # random name
        name = str(uuid.uuid4())

    if version is None:
        version = "0.0.1"

    results = load_question_json(input_dir)

    for i, result in tqdm(enumerate(results), desc="Processing results"):
        inputs = []
        for question in result["questions"]:
            inputs.append(
                {
                    "messages": [HumanMessage(content=question)],
                    "user_database_env": "duckdb",
                    "best_practice_query": "",
                }
            )
        response = graph.batch(inputs)
        answers = []
        for res in response:
            refined_input_content = (
                res["refined_input"].content
                if hasattr(res["refined_input"], "content")
                else res["refined_input"]
            )
            answers.append(
                {
                    "user_database_env": res["user_database_env"],
                    "answer_SQL": res["generated_query"],
                    "answer_explanation": res["messages"][-1].content,
                    "question_refined": refined_input_content,
                    "searched_tables": res["searched_tables"],
                }
            )

        # debug 모드일 때 결과를 print로 확인
        if debug:
            print(f"질문: {result['questions']}")
            print(f"답변: {answers}")

        result["answers"] = answers
        result["name"] = name
        result["version"] = version
        result["desc"] = desc

        save_answer_json(result, f"{output_dir}/{name}_{version}", i)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--input_dir", type=str, default="data/questions")
    parser.add_argument("--output_dir", type=str, default="data/q_sql")
    parser.add_argument("--name", type=str, default=None)
    parser.add_argument("--version", type=str, default=None)
    parser.add_argument("--desc", type=str, default="")
    parser.add_argument("--debug", type=bool, default=False)
    args = parser.parse_args()

    graph = builder.compile()  # langgraph 모델 load하여 사용하세요

    get_eval_result(
        graph,
        name=args.name,
        version=args.version,
        desc=args.desc,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        debug=args.debug,
    )
