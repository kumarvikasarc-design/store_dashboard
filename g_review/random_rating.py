import random
import pandas as pd
import os

def generate_reviews_csv(review_count, rating, output_dir):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    total_score = int(round(review_count * rating))

    ratings = []
    remaining_reviews = review_count
    remaining_score = total_score

    for _ in range(review_count - 1):
        min_possible = (remaining_reviews - 1) * 1
        max_possible = (remaining_reviews - 1) * 5

        low = int(max(1, remaining_score - max_possible))
        high = int(min(5, remaining_score - min_possible))

        r = random.randint(low, high)
        ratings.append(r)

        remaining_score -= r
        remaining_reviews -= 1

    ratings.append(int(remaining_score))
    random.shuffle(ratings)

    df = pd.DataFrame({
        "Review_Count": range(1, review_count + 1),
        "Rating": ratings
    })

    filename = f"reviews_{review_count}_rating_{rating}.csv"
    file_path = os.path.join(output_dir, filename)

    df.to_csv(file_path, index=False)
    return df, file_path


# ================= USER INPUT =================
review_count = int(input("Enter total review count: "))
rating = float(input("Enter overall rating (1 to 5): "))

output_dir = r"C:\Users\ACER\store_dashboard\g_review"

df, path = generate_reviews_csv(review_count, rating, output_dir)

print("CSV file created successfully!")
print("File saved at:", path)
print("Average Rating:", df["Rating"].mean())
