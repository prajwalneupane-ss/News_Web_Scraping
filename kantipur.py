import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
from sqlalchemy import NVARCHAR, DateTime,DATETIME
from datetime import datetime
from mtranslate import translate

def translate_nepali_to_english(text):
    try:
        translated_text = translate(text, 'en', 'ne')
        return translated_text
    except Exception as e:
        print(f"Translation error: {e}")
        return None
#No. of news required
no_of_news = 2

# Function to scrape the main page of ekantipur.com and find headlines with URLs
def scrape_main_page(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    headline_elements = soup.find_all('h2')
    headlines_urls = []
    for headline_element in headline_elements[:no_of_news]:  # Limiting to two headlines for testing
        headline = headline_element.text.strip()
        anchor = headline_element.find('a')
        if anchor:
            headline_url = anchor.get('href')
            # Check if the URL is relative or absolute
            if headline_url.startswith('https://ekantipur.com'):
                headlines_urls.append((headline, headline_url))
            else:
                full_url = 'https://ekantipur.com' + headline_url
                headlines_urls.append((headline, full_url))
    return headlines_urls

def convert_nepali_to_english(nepali_date_str):
    import nepali_datetime
    # Define a mapping of Nepali months to their corresponding numbers
    nepali_month_map = {
        'वैशाख': 1, 'जेठ': 2, 'असार': 3, 'श्रावण': 4,
        'भदौ': 5, 'असोज': 6, 'कार्तिक': 7, 'मंसिर': 8,
        'पुष': 9, 'माघ': 10, 'फाल्गुन': 11, 'चैत': 12
    }

    # Split the Nepali date string into parts
    parts = nepali_date_str.split()

    # Extract Nepali month, day, year, and time
    nepali_month = parts[0]
    nepali_day = parts[1][:-1]  # Remove the comma from the day part
    nepali_year = parts[2]
    
    nepali_time = parts[3]
    nepali_hour, nepali_minute = nepali_time.split(":")
    
    nepali_second = '००'
    # Get the corresponding Gregorian month number
    gregorian_month = nepali_month_map[nepali_month]

    # Define a mapping of Nepali digits to English digits
    nepali_digits = ['०', '१', '२', '३', '४', '५', '६', '७', '८', '९']
    english_digits = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

    # Convert Nepali digits in the year to English digits
    nepali_year = ''.join(english_digits[nepali_digits.index(d)] for d in nepali_year)

    nepali_day = ''.join(english_digits[nepali_digits.index(d)] for d in nepali_day)
    nepali_hour = ''.join(english_digits[nepali_digits.index(d)] for d in nepali_hour)
    nepali_minute = ''.join(english_digits[nepali_digits.index(d)] for d in nepali_minute)
    nepali_second = ''.join(english_digits[nepali_digits.index(d)] for d in nepali_second)

    # Create the English date string
    english_date_str = f"{nepali_year}-{gregorian_month:02d}-{nepali_day} {nepali_hour}:{nepali_minute}:{nepali_second}"

    date_part, time_part = english_date_str.split()

# Extract year, month, and day from the date part
    year, month, day = map(int, date_part.split('-'))

# If month or day has leading zero, convert it to integer without leading zero
    month = int(month)
    day = int(day)



    english_date = nepali_datetime.date(year, month, day).to_datetime_date()
    final_english_date = f"{english_date} {time_part}"

    return final_english_date

# Function to scrape detailed information from a news article URL
def scrape_detail_page(article_url):

    
    try:
        response = requests.get(article_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        category_element = soup.find('div', class_='cat_name')
        category = category_element.text.strip() if category_element else None
        
        # Check if article content exists before trying to extract text
        article_element = soup.find('div', class_='description').find_all('p')
        

        # Initialize a variable to store the formatted paragraphs
        article = ""

        # Concatenate each paragraph to the formatted output
        for paragraph in article_element:
            article += f"{paragraph.text}\n\n"
        
        article = article.split('—')[1].strip() # Add two newlines for separation
            

        # Print the formatted output
        
        
        # Extracting additional information
        source = 'कान्तिपुर'
        district_element = soup.find('div', class_='description').find('p')
        district = district_element.text.split('—')[0].strip() if district_element else None
        author_element = soup.find('span', class_='author')
        author = author_element.text.strip() if author_element else None


        title_element = soup.find_all('div', class_='sub-headline')
        

        titles = []

        # Extract text from each div element and append to the titles list
        for subheading in title_element:
            titles.append(subheading.text.strip())

        # Join the titles with a space separator
        title = ' '.join(titles) if title_element else article.split('\n\n')[0]

       
        
        published_date_element = soup.find('span', class_='published-at')
        published_date_unformat = published_date_element.text.split(':') 
        published_date_raw = ':'.join(published_date_unformat[1:]).strip()
        
        published_date = convert_nepali_to_english(published_date_raw) if published_date_element else None
        
        date_format = '%Y-%m-%d %H:%M:%S'

        # Convert the string to a datetime object using strptime()
        datetime_obj = datetime.strptime(published_date, date_format)

        # Store the datetime object in a variable
        published_date = datetime_obj
        
        

       
        
        return title, category, article, district, source, author, published_date
    except Exception as e:
        print(f"Error scraping detail page: {e}")
        return None

# Main function to orchestrate the scraping and extraction process
def main_pipeline():
    base_url = 'https://ekantipur.com'
    headlines_urls = scrape_main_page(base_url)
    if not headlines_urls:
        print("No headlines or URLs found.")
        return pd.DataFrame(), pd.DataFrame()  # Return empty DataFrames
    nepali_data = []
    english_data = []
    for headline, url in headlines_urls:
        print(f"Processing: {headline} - {url}")
        result = scrape_detail_page(url)
        if result:
            title, category, article, district, source, author, published_date = result
            # Translate Nepali text to English
            source_en = translate_nepali_to_english(source)
            headline_en = translate_nepali_to_english(headline)
            title_en = translate_nepali_to_english(title)
            category_en = translate_nepali_to_english(category)
            article_en = translate_nepali_to_english(article)
            district_en = translate_nepali_to_english(district)
            author_en = translate_nepali_to_english(author)

            nepali_data.append({
                'Source': source,
                'Headline': headline,
                'Title': title,
                'Category': category,
                'URL': url,
                'Article': article,
                'Author': author,
                'District': district,
                'Published_Date': published_date
            })

            english_data.append({
                'Source': source_en,
                'Headline': headline_en,
                'Title': title_en,
                'Category': category_en,
                'URL': url,
                'Article': article_en,
                'Author': author_en,
                'District': district_en,
                'Published_Date': published_date
            })

    nepali_df = pd.DataFrame(nepali_data)
    english_df = pd.DataFrame(english_data)

    return nepali_df, english_df
    



# Run the main pipeline
nepali_df, english_df = main_pipeline()

print(nepali_df)



