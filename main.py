import requests
import pandas as pd
from datetime import datetime
import os
from google.cloud import storage
import logging
import time
from typing import Optional, Dict, Any
import sys

# 設定logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

class WeatherAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://opendata.cwa.gov.tw/api"
        self.forecast_endpoint = "/v1/rest/datastore/F-C0032-001"
        # 定義台灣所有縣市
        self.locations = [
            '宜蘭縣', '花蓮縣', '臺東縣',
            '澎湖縣', '金門縣', '連江縣',
            '臺北市', '新北市', '桃園市',
            '臺中市', '臺南市', '高雄市',
            '基隆市', '新竹縣', '新竹市',
            '苗栗縣', '彰化縣', '南投縣',
            '雲林縣', '嘉義縣', '嘉義市',
            '屏東縣'
        ]

    def get_weather_forecast(self) -> Optional[Dict[str, Any]]:
        """獲取天氣預報資料"""
        try:
            url = f"{self.base_url}{self.forecast_endpoint}"
            params = {
                "Authorization": self.api_key,
                "locationName": ','.join(self.locations)  # 請求所有縣市的資料
            }
            
            logger.info(f"Making API request for {len(self.locations)} locations")
            logger.info(f"Using API key (first 5 chars): {self.api_key[:5]}...")
            
            response = requests.get(url, params=params, timeout=30)
            
            logger.info(f"API Response Status Code: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"API Error Response: {response.text}")
                return None
            
            data = response.json()
            logger.info(f"Received data success status: {data.get('success', False)}")
            
            if 'records' in data and 'location' in data['records']:
                location_count = len(data['records']['location'])
                logger.info(f"Found {location_count} locations in response")
            
            return data
            
        except Exception as e:
            logger.error(f"Error in get_weather_forecast: {str(e)}", exc_info=True)
            return None

    def parse_forecast_data(self, data: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """解析天氣預報數據"""
        try:
            if not data.get('success', False):
                logger.error("API response indicates failure")
                return None

            locations = data['records']['location']
            forecast_data = []

            for location in locations:
                county = location['locationName']
                logger.info(f"Processing data for {county}")
                
                # 取得天氣預報時間範圍
                weather_time = location['weatherElement'][0]['time'][0]
                start_time = weather_time['startTime']
                end_time = weather_time['endTime']
                
                weather_data = {
                    'county': county,
                    'start_time': start_time,
                    'end_time': end_time,
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # 處理各項天氣要素
                for element in location['weatherElement']:
                    element_name = element['elementName']
                    time_data = element['time'][0]  # 取得最近一次預報
                    parameter = time_data['parameter']
                    
                    # 根據不同天氣要素給予不同的欄位名稱
                    if element_name == 'Wx':  # 天氣現象
                        weather_data['weather_description'] = parameter['parameterName']
                        weather_data['weather_code'] = parameter['parameterValue']
                    elif element_name == 'PoP':  # 降雨機率
                        weather_data['rain_probability'] = parameter['parameterName']
                    elif element_name == 'MinT':  # 最低溫度
                        weather_data['min_temperature'] = parameter['parameterName']
                    elif element_name == 'MaxT':  # 最高溫度
                        weather_data['max_temperature'] = parameter['parameterName']
                    elif element_name == 'CI':  # 舒適度
                        weather_data['comfort_index'] = parameter['parameterName']
                    
                forecast_data.append(weather_data)

            # 創建 DataFrame 並排序
            df = pd.DataFrame(forecast_data)
            df = df.sort_values('county')
            
            logger.info(f"Created DataFrame with {len(df)} locations")
            logger.info(f"Columns: {df.columns.tolist()}")
            
            return df

        except Exception as e:
            logger.error(f"Error in parse_forecast_data: {str(e)}", exc_info=True)
            return None

def upload_to_gcs(bucket_name: str, data: pd.DataFrame, filename: str) -> bool:
    """上傳資料到 Google Cloud Storage"""
    try:
        logger.info(f"Initializing GCS client")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"weather/{filename}")
        #blob = bucket.blob(filename)
        
        # 將 DataFrame 轉換為 CSV
        csv_data = data.to_csv(index=False)
        logger.info(f"CSV data size: {len(csv_data)} bytes")
        
        # 上傳檔案
        blob.upload_from_string(csv_data, content_type='text/csv')
        
        # 驗證上傳
        if blob.exists():
            logger.info(f"Successfully uploaded file. Size: {blob.size} bytes")
            return True
        else:
            logger.error("File upload verification failed")
            return False
            
    except Exception as e:
        logger.error(f"Error in upload_to_gcs: {str(e)}", exc_info=True)
        return False

def main():
    try:
        # 取得環境變數
        api_key = os.environ.get('CWB_API_KEY')
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        
        logger.info("Starting weather data collection process")
        logger.info(f"Using bucket name: {bucket_name}")
        
        if not api_key or not bucket_name:
            raise ValueError("Missing required environment variables")

        # 初始化 API 客戶端
        weather_api = WeatherAPI(api_key)
        
        # 獲取天氣資料
        logger.info("Fetching weather forecast data")
        forecast_data = weather_api.get_weather_forecast()
        
        if not forecast_data:
            raise Exception("Failed to fetch weather data")
        
        # 解析資料
        logger.info("Parsing weather data")
        df = weather_api.parse_forecast_data(forecast_data)
        
        if df is None or df.empty:
            raise Exception("No weather data parsed")
            
        # 產生檔案名稱
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"weather_forecast_{timestamp}.csv"
        
        # 上傳到 GCS
        logger.info(f"Uploading data to GCS: {filename}")
        if not upload_to_gcs(bucket_name, df, filename):
            raise Exception("Failed to upload data to GCS")
            
        logger.info("Weather data collection completed successfully")

    except Exception as e:
        logger.error(f"Error in main function: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()

