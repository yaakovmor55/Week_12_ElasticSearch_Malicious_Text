from nltk.sentiment.vader import SentimentIntensityAnalyzer as sia
import nltk
nltk.download('vader_lexicon', quiet=True)





class SentimentRecognition:
    """
           Analyze the sentiment of the text using VADER.
    """

    @staticmethod
    def sentiment_analyzer(text):
        try:
            score = sia().polarity_scores(text)
            compound = score["compound"]
            if compound >= 0.5:
                sentiment = "positive"
            elif compound <= -0.5:
                sentiment = "negative"
            else:
                sentiment = "neutral"
            return sentiment
        except Exception as e:
            print(f"Error analyzing sentiment: {e}")
            return None

