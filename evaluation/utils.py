import json
import os
from glob import glob

from persona_class import PersonaList


def save_persona_json(data, filepath):
    dir_path = os.path.dirname(filepath)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data.model_dump(), f, ensure_ascii=False, indent=4)


def load_persona_json(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return PersonaList(**json.load(f))


def save_question_json(data, filepath):
    dir_path = os.path.dirname(filepath)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    data["persona"] = (
        data["persona"].model_dump()
        if hasattr(data["persona"], "model_dump")
        else data["persona"].__dict__
    )
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_question_json(dir_path):
    restult_path = glob(f"{dir_path}/*.json")
    results = []
    for path in restult_path:
        with open(path, "r") as f:
            results.append(json.load(f))
    return results


def save_answer_json(data, filepath, index):
    if not os.path.exists(filepath):
        os.makedirs(filepath)
    with open(f"{filepath}/eval_result_{index}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def pretty_print_persona(persona):
    return f"""
    Name: {persona.name}
    Department: {persona.department}
    Role: {persona.role}
    Background: {persona.background}
    """
