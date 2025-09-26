import re
import pandas as pd
from transformers import pipeline

# === Load parquet ===
df = pd.read_parquet("scraped_7d.parquet", engine="pyarrow")

# === Define models ===
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

# === Candidate categories ===
categories = [
    'Electronics', 'Fashion', 'Home Goods', 'Beauty', 
    'Sports', 'Books', 'Groceries', 'Vehicles','Medicine'
]

# === Text cleaner ===
def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    # Remove URLs
    text = re.sub(r"http\S+", "", text)
    # Remove extra whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# === Function to enrich product ===
def enrich_product(title, desc):
    text = desc if isinstance(desc, str) and len(desc) > 10 else title
    text = clean_text(text)

    # Category classification
    classification = classifier(text, candidate_labels=categories)
    category = classification["labels"][0]

    # Summarized description
    try:
        summary = summarizer(text, max_length=40, min_length=10, do_sample=False)[0]["summary_text"]
    except Exception:
        summary = text[:80]  # fallback if summarizer fails

    return category, summary

# === Apply enrichment ===
df[["predicted_category", "generated_description"]] = df.apply(
    lambda row: pd.Series(enrich_product(row["title"], row["description"])),
    axis=1
)

# === Save back ===
df.to_parquet("products_enriched.parquet", engine="pyarrow", index=False)
print("✅ Saved products_enriched.parquet with categories + descriptions")
