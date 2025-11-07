import streamlit as st
import pandas as pd
import joblib
from sqlalchemy.engine import create_engine

# -----------------------------
# Config
# -----------------------------
MODEL_PATH = "recommendation_model.pkl"
TRINO_URI = "trino://anoir@localhost:8086/iceberg/gold_layer"

TOP_N_DEFAULT = 10
RATING_LIKE_THRESHOLD = 4

st.set_page_config(page_title="Movie Recommendations", page_icon="🎬", layout="wide")

# -----------------------------
# Load data from Trino
# -----------------------------
@st.cache_data(show_spinner=False)
def load_data():
    engine = create_engine(TRINO_URI)
    movies = pd.read_sql("SELECT * FROM dim_movies", engine)
    users = pd.read_sql("SELECT * FROM dim_users", engine)
    ratings = pd.read_sql("SELECT * FROM fact_ratings", engine)

    # Normalize column names
    movies.columns = [c.strip().lower() for c in movies.columns]
    users.columns = [c.strip().lower() for c in users.columns]
    ratings.columns = [c.strip().lower() for c in ratings.columns]

    # Split genres
    movies["genres"] = movies["genres"].fillna("")
    movies["genres_list"] = movies["genres"].apply(lambda s: [g.strip() for g in s.split("|") if g.strip()])

    # Add liked flag
    ratings["liked"] = (ratings["rating"] >= RATING_LIKE_THRESHOLD).astype(int)

    return users, movies, ratings

@st.cache_resource(show_spinner=False)
def load_model():
    return joblib.load(MODEL_PATH)

# -----------------------------
# Recommendation function
# -----------------------------
def recommend_for_user(clf, users, movies, ratings, user_id, top_n=10):
    if user_id not in set(users["user_id"].unique()):
        return pd.DataFrame(columns=["movie_id", "title", "score"]), "User ID not found."

    user_row = users.loc[users["user_id"] == user_id].iloc[0]
    rated_movie_ids = set(ratings.loc[ratings["user_id"] == user_id, "movie_id"].unique())
    candidates = movies[~movies["movie_id"].isin(rated_movie_ids)].copy()

    if candidates.empty:
        return pd.DataFrame(columns=["movie_id", "title", "score"]), "No candidate movies left."

    # Attach user features
    candidates["gender"] = user_row["gender"]
    candidates["age"] = user_row["age"]
    candidates["occupation"] = user_row["occupation"]

    # Multi-hot genres
    genre_set = set()
    for gl in movies["genres_list"]:
        genre_set.update(gl)
    genre_cols = sorted(list(genre_set))
    for g in genre_cols:
        candidates[g] = candidates["genres_list"].apply(lambda lst: 1 if g in lst else 0)

    X_cand = candidates[["gender", "age", "occupation"] + genre_cols]

    scores = clf.predict_proba(X_cand)[:, 1]
    candidates["score"] = scores

    recs = candidates.sort_values("score", ascending=False)[["movie_id", "title", "genres", "score"]].head(top_n)
    return recs, None

# -----------------------------
# User history + preferences
# -----------------------------
def get_user_history(ratings, movies, user_id):
    hist = ratings[ratings["user_id"] == user_id].merge(movies[["movie_id", "title", "genres"]], on="movie_id", how="left")
    return hist.sort_values("timestamp", ascending=False)

def compute_user_genre_pref(user_history):
    if user_history.empty:
        return pd.DataFrame(columns=["genre", "count"])
    exploded = user_history.assign(genre=user_history["genres"].str.split("|")).explode("genre")
    exploded["genre"] = exploded["genre"].str.strip()
    exploded = exploded[exploded["genre"] != ""]
    agg = exploded.groupby("genre")["liked"].sum().reset_index().rename(columns={"liked": "count"})
    return agg.sort_values("count", ascending=False)

# -----------------------------
# Catalog analytics
# -----------------------------
def top_popular_movies(ratings, movies, top_n=15):
    pop = (
        ratings.groupby("movie_id")
        .agg(count=("rating", "count"), avg_rating=("rating", "mean"))
        .reset_index()
        .merge(movies[["movie_id", "title", "genres"]], on="movie_id", how="left")
        .sort_values(["count", "avg_rating"], ascending=[False, False])
        .head(top_n)
    )
    return pop

# -----------------------------
# Streamlit UI
# -----------------------------
st.title("🎬 Movie Recommendation Dashboard")

users, movies, ratings = load_data()
clf = load_model()

# KPIs
col1, col2, col3, col4 = st.columns(4)
col1.metric("Users", f"{users['user_id'].nunique():,}")
col2.metric("Movies", f"{movies['movie_id'].nunique():,}")
col3.metric("Ratings", f"{len(ratings):,}")
col4.metric("Avg Rating", f"{ratings['rating'].mean():.2f}")

st.divider()

# User input
st.subheader("Recommendations")
user_id_input = st.text_input("Enter User ID")
top_n = st.slider("Top-N recommendations", 1, 20, TOP_N_DEFAULT)

if st.button("Recommend"):
    try:
        user_id = int(user_id_input)
        recs, err = recommend_for_user(clf, users, movies, ratings, user_id, top_n=top_n)
        if err:
            st.warning(err)
        else:
            st.success(f"Top {len(recs)} recommendations for user {user_id}")
            st.dataframe(recs, use_container_width=True)

            # User profile and history
            st.subheader("User Profile & History")
            user_row = users.loc[users["user_id"] == user_id].iloc[0]
            uc1, uc2, uc3 = st.columns(3)
            uc1.metric("Gender", str(user_row.get("gender", "")))
            uc2.metric("Age", int(user_row.get("age", 0)))
            uc3.metric("Occupation", str(user_row.get("occupation", "")))

            user_hist = get_user_history(ratings, movies, user_id)
            if not user_hist.empty:
                st.markdown("Recent Ratings")
                st.dataframe(user_hist[["movie_id", "title", "genres", "rating", "timestamp"]].head(20), use_container_width=True)

                st.markdown("User Genre Preferences")
                pref = compute_user_genre_pref(user_hist)
                if not pref.empty:
                    st.bar_chart(pref.set_index("genre")["count"])
                else:
                    st.info("No genre preferences available for this user yet.")
            else:
                st.info("No ratings found for this user.")
    except ValueError:
        st.error("Please enter a valid integer user ID.")

st.divider()

# Catalog analytics
st.subheader("Catalog Analytics")

c1, c2 = st.columns(2)
with c1:
    st.markdown("📊 Genre distribution across catalog")
    genre_counts = pd.Series([g for gl in movies["genres_list"] for g in gl]).value_counts()
    if not genre_counts.empty:
        st.bar_chart(genre_counts)
    else:
        st.info("No genres found in catalog.")

with c2:
    st.markdown("⭐ Rating distribution")
    rating_dist = ratings["rating"].value_counts().sort_index()
    if not rating_dist.empty:
        st.bar_chart(rating_dist)
    else:
        st.info("No ratings available.")

st.markdown("🔥 Top Popular Movies (by rating count & average rating)")
popular = top_popular_movies(ratings, movies, top_n=15)
st.dataframe(popular, use_container_width=True)
