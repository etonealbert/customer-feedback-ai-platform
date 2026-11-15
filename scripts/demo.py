"""Submit feedback, await inference, ask a grounded question, verify isolation."""

import time
from pathlib import Path
from uuid import uuid4

import httpx
from dotenv import dotenv_values


def main():
    env = dotenv_values(Path(__file__).resolve().parents[1] / ".env")
    headers = {"Authorization": f"Bearer {env['DEMO_TENANT_A_KEY']}"}
    with httpx.Client(timeout=15) as client:
        response = client.post(
            "http://localhost:8000/v1/feedback",
            headers={**headers, "Idempotency-Key": str(uuid4())},
            json={
                "text": "The mobile application keeps crashing during checkout and payment fails.",
                "source": "mobile",
            },
        )
        response.raise_for_status()
        feedback_id = response.json()["id"]
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            response = client.get(
                f"http://localhost:8000/v1/feedback/{feedback_id}", headers=headers
            )
            response.raise_for_status()
            result = response.json()
            if result["status"] != "pending":
                break
            time.sleep(0.5)
        if result["status"] != "completed":
            raise RuntimeError(f"Feedback did not complete: {result['status']}")
        print("Feedback:", feedback_id)
        print("Prediction:", result["prediction"])
        answer = client.post(
            "http://localhost:8002/v1/ask",
            headers=headers,
            json={"question": "What complaints concern the mobile application?", "limit": 5},
        )
        answer.raise_for_status()
        print("Answer mode:", answer.json()["mode"])
        print(answer.json()["answer"])
        other = client.get(
            f"http://localhost:8000/v1/feedback/{feedback_id}",
            headers={"Authorization": f"Bearer {env['DEMO_TENANT_B_KEY']}"},
        )
        assert other.status_code == 404, "Cross-tenant access unexpectedly succeeded"
        print("Tenant isolation: other tenant receives 404.")


if __name__ == "__main__":
    main()
