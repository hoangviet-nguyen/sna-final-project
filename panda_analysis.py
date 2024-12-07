import pandas as pd
from ydata_profiling import ProfileReport

# Load the Anime Studio dataset
df = pd.read_csv('data/Anime_Studio.csv', encoding='latin1')

# Generate the profiling report for Anime Studio
profile = ProfileReport(
    df,
    title='Pandas Profiling Report',
    dataset={
        "description": "This profiling report was generated for the anime studio dataset using data from the Jikan API, a wrapper and scraper of MyAnimeList.",
        "url": "https://jikan.moe",
        "source": "Data sourced from MyAnimeList via the Jikan API."
    },
    variables={
        "descriptions": {
            "rank": "It contains the rank of the anime.",
            "anime": "It contains the name of the anime.",
            "studios": "It contains the name of the studio that produced the anime.",
        }
    },
)
profile.to_file('pandaReport/panda_anime_studio_report.html')

# Load the Anime Producer dataset
df_producer = pd.read_csv('data/Anime_Producer.csv', encoding='latin1')

# Generate the profiling report for Anime Producer
profile_producer = ProfileReport(
    df_producer,
    title='Anime Producer Profiling Report',
    dataset={
        "description": "This profiling report was generated for the anime producer dataset using data from the Jikan API, a wrapper and scraper of MyAnimeList.",
        "url": "https://jikan.moe",
        "source": "Data sourced from MyAnimeList via the Jikan API."
    },
    variables={
        "descriptions": {
            "rank": "It contains the rank of the anime.",
            "anime": "It contains the name of the anime.",
            "producers": "It contains the name of the producer that produced the anime.",
        }
    },
)
profile_producer.to_file('pandaReport/panda_anime_producer_report.html')

# Load the Anime Voice Actor dataset
df_voiceactor = pd.read_csv('data/Anime_Voice_Actor.csv', encoding='latin1')

# Generate the profiling report for Anime Voice Actor
profile_voiceactor = ProfileReport(
    df_voiceactor,
    title='Anime Voice Actor Profiling Report',
    dataset={
        "description": "This profiling report was generated for the anime voice actor dataset using data from the Jikan API, a wrapper and scraper of MyAnimeList.",
        "url": "https://jikan.moe",
        "source": "Data sourced from MyAnimeList via the Jikan API."
    },
    variables={
        "descriptions": {
            "rank": "It contains the rank of the anime.",
            "anime": "It contains the name of the anime.",
            "voice_actors": "It contains the name of the voice actor that voiced the character in the anime.",
        }
    },
)
profile_voiceactor.to_file('pandaReport/panda_anime_voiceactor_report.html')
