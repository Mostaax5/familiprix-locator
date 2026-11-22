"""Repeatable synthetic identity regression benchmark. No API or store access."""
import json
from pathlib import Path
from product_data import assess_metadata_candidate


def evaluate(assessor=assess_metadata_candidate):
    cases = json.loads((Path(__file__).parent / "evaluation" / "identity_cases.json").read_text())
    metrics = {"cases": len(cases), "correct": 0, "wrong_auto_associations": 0,
               "valid_associations_withheld": 0, "failures": []}
    for label, existing, incoming, expected in cases:
        result = assessor(
            {"barcode": "063848966068", "brand": "Acme", "name": existing},
            {"barcode": "063848966068", "brand": "Acme", "name": incoming,
             "source": "Manufacturer exact product"},
        )
        actual = bool(result.auto_apply)
        if actual == expected:
            metrics["correct"] += 1
        else:
            metrics["wrong_auto_associations" if actual else "valid_associations_withheld"] += 1
            metrics["failures"].append(label)
    metrics["dataset"] = "Synthetic regression cases; not measured real-store accuracy"
    return metrics


if __name__ == "__main__":
    metrics = evaluate()
    print(json.dumps(metrics, indent=2))
    raise SystemExit(bool(metrics["failures"]))
