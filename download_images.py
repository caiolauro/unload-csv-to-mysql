import os
import pandas as pd
import requests
from PIL import Image
from concurrent.futures import ThreadPoolExecutor

def download_image(row):
    guid, download_url, save_as_name = row

    save_path = f"output/images/{save_as_name}"

    if os.path.exists(save_path):
        print(f"Image {save_as_name} already exists. Skipping download.")
        return save_as_name, True

    response = requests.get(download_url, stream=True)
    try:
        if response.status_code == 200:
            with Image.open(response.raw) as img:
                img.save(save_path)
            return save_as_name, True
        else:
            print(f'Request issue - Image {save_as_name} couldn\'t be retrieved')
            return save_as_name, False
    except Exception as e:
        print(e)
        return save_as_name, False

if __name__ == "__main__":
    images_to_scrape_df = pd.read_csv("input/images_to_scrape.csv")
    list_of_failed_downloads = []

    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(download_image, row) for _, row in images_to_scrape_df.iterrows()]

        for future in futures:
            save_as_name, success = future.result()
            if not success:
                list_of_failed_downloads.append(save_as_name)

    print(f"Failed {len(list_of_failed_downloads)} images.")
    print(f"Success {len(images_to_scrape_df) - len(list_of_failed_downloads)} images.")

    # Write the list of failed downloads to a CSV file
    failed_downloads_df = pd.DataFrame({"FailedDownloads": list_of_failed_downloads})
    failed_downloads_df.to_csv("output/failed_downloads.csv", index=False)
