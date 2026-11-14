"""Write the small, synthetic, hand-authored demonstration corpus.

The final sentence in each group is held out, never fitted by a classifier or
embedding model. This is a reproducible smoke-test dataset, not a population
sample or a defensible estimate of real customer sentiment accuracy.
"""

import json
from pathlib import Path


EXAMPLES = {
    "performance": {
        "positive": [
            "The mobile app is fast and pages load instantly.",
            "I love how quickly the dashboard opens now.",
            "Excellent speed, even with a large account.",
            "The latest update made scrolling smooth and responsive.",
            "Search results appear immediately and the app feels great.",
            "The application is wonderfully quick on my older phone.",
        ],
        "negative": [
            "The mobile app is painfully slow and pages take forever to load.",
            "The dashboard has terrible lag when I scroll.",
            "Search takes ages and wastes my time.",
            "The application is sluggish and drains my battery.",
            "I hate waiting for the slow loading screen.",
            "Mobile performance is awful with long delays after every tap.",
        ],
        "neutral": [
            "The dashboard takes about three seconds to load.",
            "I checked the loading time on two phones.",
            "The app uses battery while synchronizing data.",
            "Search speed depends on the number of records.",
            "I measured the page response time this morning.",
            "The application loads the dashboard before the activity page.",
        ],
    },
    "reliability": {
        "positive": [
            "The mobile app is stable and never crashes anymore.",
            "Great update, the login bug is fixed.",
            "I am happy that synchronization works reliably.",
            "Excellent reliability, my data saves every time.",
            "The application recovered my work perfectly after reconnecting.",
            "I love that the app now stays connected without errors.",
        ],
        "negative": [
            "The mobile app crashes every time I open it.",
            "I cannot log in because of a broken authentication screen.",
            "Synchronization fails and I lost all my saved work.",
            "The app freezes constantly and shows error messages.",
            "The latest release is buggy and keeps disconnecting.",
            "Terrible mobile crashes make the application unusable.",
        ],
        "neutral": [
            "The app reconnects after the network changes.",
            "I restarted the application after installing the update.",
            "The login screen asks for a verification code.",
            "Synchronization runs every ten minutes.",
            "The error log contains a connection message.",
            "My phone reconnects to the service when Wi-Fi returns.",
        ],
    },
    "billing": {
        "positive": [
            "The subscription is excellent value for the price.",
            "I appreciate the clear invoice and fair charges.",
            "The refund arrived quickly and I am happy.",
            "Upgrading my payment plan was simple and affordable.",
            "Great pricing with no unexpected fees.",
            "I love the reasonable monthly subscription cost.",
        ],
        "negative": [
            "I was charged twice and the subscription is too expensive.",
            "The invoice has hidden fees that are unacceptable.",
            "My refund is missing and billing support is useless.",
            "I hate the unexpected price increase on my plan.",
            "The payment failed but you still charged my card.",
            "Awful billing, another duplicate charge appeared on my invoice.",
        ],
        "neutral": [
            "My subscription renews on the first of each month.",
            "The invoice is available in the billing section.",
            "I changed the credit card for my payment plan.",
            "The monthly price is listed on my account.",
            "I downloaded the receipt for the last charge.",
            "Our annual subscription invoice contains the payment date.",
        ],
    },
    "usability": {
        "positive": [
            "The interface is beautiful and easy to navigate.",
            "I love the clear buttons and intuitive menu.",
            "The new mobile layout makes everything simple to find.",
            "Excellent design, setting up my account was effortless.",
            "The readable text and accessible controls are great.",
            "The application navigation is wonderfully simple and clear.",
        ],
        "negative": [
            "The interface is confusing and I cannot find the settings.",
            "The menu is cluttered and the tiny buttons are hard to tap.",
            "I hate the unreadable text in the new mobile design.",
            "The onboarding screens are frustrating and complicated.",
            "The layout is terrible and navigation makes no sense.",
            "The confusing mobile navigation hides every useful control.",
        ],
        "neutral": [
            "The settings button is in the account menu.",
            "The mobile layout has three tabs at the bottom.",
            "I switched the interface to dark mode.",
            "The onboarding flow contains four screens.",
            "The text size can be changed in accessibility settings.",
            "The navigation menu appears on the left side of the screen.",
        ],
    },
    "support": {
        "positive": [
            "The support team was helpful and solved my problem quickly.",
            "Excellent customer service with a friendly response.",
            "I appreciate the patient agent who answered my questions.",
            "The help desk resolved my ticket perfectly.",
            "Great support, the chat agent explained everything clearly.",
            "Your helpful support agent gave me an excellent answer.",
        ],
        "negative": [
            "Customer support ignored my ticket for a week.",
            "The service agent was rude and unhelpful.",
            "I am frustrated that nobody answers the support chat.",
            "The help desk gave me a useless response.",
            "Terrible customer service, my issue is still unresolved.",
            "The support team never replied and my complaint remains unresolved.",
        ],
        "neutral": [
            "I opened a support ticket yesterday.",
            "The customer service chat is open during business hours.",
            "An agent sent me the help article.",
            "My ticket number is listed in the support portal.",
            "I contacted the help desk about my account.",
            "The support agent forwarded my question to another team.",
        ],
    },
    "features": {
        "positive": [
            "I love the new offline mode and export feature.",
            "The reporting tools are excellent and useful.",
            "Great integration with my calendar and notifications.",
            "The new collaboration feature makes teamwork easy.",
            "I am happy that custom reports are now supported.",
            "The offline feature is fantastic for working while travelling.",
        ],
        "negative": [
            "The missing offline mode makes the mobile app useless on trips.",
            "I am disappointed that exporting reports is not supported.",
            "The calendar integration is broken and notifications are annoying.",
            "The application lacks essential collaboration features.",
            "I hate that there is no way to customize reports.",
            "The lack of offline access is frustrating when I travel.",
        ],
        "neutral": [
            "Does the app include an offline mode?",
            "I would like to know which export formats are supported.",
            "The reporting feature includes a monthly view.",
            "Calendar integration is available in the preferences.",
            "I enabled notifications for the collaboration workspace.",
            "The export feature offers a CSV format for reports.",
        ],
    },
}


def main() -> None:
    rows = [
        {
            "id": f"{topic}-{sentiment}-{index + 1:02d}",
            "text": text,
            "sentiment": sentiment,
            "topic": topic,
            "split": "test" if index == len(sentences) - 1 else "train",
            "source": "hand-authored synthetic demonstration",
        }
        for topic, groups in EXAMPLES.items()
        for sentiment, sentences in groups.items()
        for index, text in enumerate(sentences)
    ]
    destination = Path(__file__).with_name("feedback.jsonl")
    destination.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )
    print(f"Wrote {len(rows)} rows to {destination}")


if __name__ == "__main__":
    main()
