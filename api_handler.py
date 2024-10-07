import json
from multiprocessing import Pool
import requests
import time

class AutotraderAPI:
    '''
    Interface for the Autotrader API, with some workarounds to handle records limit per API request 
    for building pretty big data fetches (50K+ records, which takes approx. 1 minute currently)
    '''
    api_base_url = 'https://www.autotrader.com/rest/lsc/listing'
    record_request_threshold = 3800

    def __init__(self):
        # self.api_req_count = 0
        pass

    def run_query(self, query_params: dict = None):
        if not query_params:
            query_params = {
                'bodyStyleSubtypeCode': 'FULLSIZE_CREW,COMPACT_CREW',
                'listingType': 'USED,CERTIFIED,3P_CERT',
                'numRecords': self.record_request_threshold,
                'searchRadius': 500,
                'vehicleStyleCode': 'TRUCKS',
                'zip': 66501,
                'minPrice': 0,
                'maxPrice': 150000
            }
        ## TODO: add more of the API params / check for valid params?
   
        subqueries_list = self.build_subqueries(query_params)

        # combine each subquery params with main params 
        subqueries_params = [{**query_params, **subquery_params} for subquery_params in subqueries_list]

        records = self.run_subqueries(subqueries_params)

        try:
            with open('autotrader_query_output.json', 'w') as f:
                json.dump(records, f, indent=4)
        except Exception as e:
            print("Error: could not save output: ", e)
        print('Output saved to json file!')

        print('Total listings fetched: ', len(records))
        # print('Requests count: ', self.api_req_count)

    def build_subqueries(self, main_query_params: dict) -> list:
        '''
        Get subqueries for price ranges 
        i.e.: [
            {'minPrice':0,'maxPrice':10465}, 
            {'minPrice':10466, 'maxPrice':17622}
            , ... , 
            {'minPrice':123789, 'maxPrice':150000}
            ]

        Currently only using 'minPrice' and 'maxPrice' as range of values to query, but could work for 
        'startYear' and 'endYear' if it's not a huge query 
        (otherwise count of results per year would probably be too high)

        Note: using a forward forward search method
        '''
        print('Started building subqueries list...')
        subquery_params = {**main_query_params, 'numRecords': 0} 
        price_subqueries = []

        # get initial record count
        query_size_req = requests.get(self.api_base_url, headers={'Accept-Encoding': 'gzip, deflate'}, params=subquery_params)
        total_remaining_records = query_size_req.json().get('totalResultCount', 0)

        print('Initial records available: ', total_remaining_records)

        # initial price step calculation
        next_price_step = int((self.record_request_threshold / total_remaining_records) * subquery_params['maxPrice'])
        subquery_params['maxPrice'] = next_price_step
        highest_seen_current = None

        # track count of each sub-step (searching each price range)
        sub_step_count = 0

        # start forward search loop
        while True:
            sub_step_count += 1
            resp = requests.get(self.api_base_url, headers={'Accept-Encoding': 'gzip, deflate'}, params=subquery_params)
            # self.api_req_count += 1
            json_resp = resp.json()
            # query_resp_listings = json_resp.get('listings', [])
            subquery_available_records = json_resp.get('totalResultCount', 0)

            current_min_price = subquery_params['minPrice']
            current_max_price = subquery_params['maxPrice']

            # stop if upper price limit reached
            if current_max_price == main_query_params['maxPrice']:
                price_subqueries.append({
                    'minPrice': current_min_price,
                    'maxPrice': current_max_price,
                    'expected_count': subquery_available_records
                })
                break

            # stop when no more records found
            if subquery_available_records == 0:
                break

            # adjust the max price if the number of query records is too high or low
            if subquery_available_records > self.record_request_threshold or \
                (subquery_available_records < self.record_request_threshold and self.record_request_threshold - subquery_available_records > 1000):

                # calculate the next step in price relative to records available
                # TODO: tweak to improve performance?
                rel_step = int((self.record_request_threshold / subquery_available_records) * next_price_step)
        
                # force step size minimum 
                min_step_size = 500
                if abs(rel_step - next_price_step) < min_step_size:
                    if rel_step < next_price_step:
                        rel_step = next_price_step - min_step_size
                    elif rel_step > next_price_step:
                        rel_step = next_price_step + min_step_size

                # Limit step increase to 2x the previous step to avoid too large jumps
                next_price_step = min(rel_step, next_price_step * 2)

                # Update highest_seen_current only if the record count exceeds the threshold
                if subquery_available_records > self.record_request_threshold:
                    if highest_seen_current is None:
                        highest_seen_current = current_max_price
                    elif current_max_price > highest_seen_current:
                        # print(f'Records exceed threshold! Updating highest_seen_current: {current_max_price}')
                        highest_seen_current = current_max_price

                # check if step is too high when increasing the price step (too few records)
                if highest_seen_current is not None and next_price_step > highest_seen_current:
                    # print(f'Next step ({next_price_step}) too high! Highest seen current = {highest_seen_current}')
                    next_price_step = int((highest_seen_current - current_max_price) / 2) + current_max_price

                subquery_params['maxPrice'] = current_min_price + next_price_step

                # Ensure the maxPrice does not exceed the main maxPrice
                if subquery_params['maxPrice'] > main_query_params['maxPrice']:
                    subquery_params['maxPrice'] = main_query_params['maxPrice']

            else:
                # if number of records is within threshold, save subquery and move to the next range
                price_subqueries.append({
                    'minPrice': current_min_price,
                    'maxPrice': current_max_price,
                    # 'expected_count': subquery_available_records,
                    # 'substep_count': sub_step_count
                })
                highest_seen_current = None
                sub_step_count = 0
                total_remaining_records -= subquery_available_records

                # set the starting price range for the next "bucket" of price ranges
                # assuming a normalish distribution, just initially repeat the last range that worked
                next_price_step = current_max_price - current_min_price
                subquery_params['minPrice'] = current_max_price + 1
                subquery_params['maxPrice'] = current_max_price + next_price_step \
                    if current_max_price + next_price_step < main_query_params['maxPrice'] else main_query_params['maxPrice']

        # print(f'\nPrice subqueries (count: {len(price_subqueries)}):')
        # for q in price_subqueries:
        #     print(q)
        # print(f'Request count: {self.api_req_count}')
        print('Finished building subqueries list...')
        
        return [{k:v for k, v in q.items() if k in ('minPrice', 'maxPrice')} for q in price_subqueries]
       
    def make_request(self, api_params: dict) -> list:
        '''
        Request records from the api, using 'minPrice' and 'maxPrice' params
        '''
        # send request and check response, resend up to 5 times with a delay if response empty
        retry_count = 5
        while True:
            response = requests.get(self.api_base_url, headers={'Accept-Encoding': 'gzip, deflate'}, params=api_params, timeout=10)
            # self.api_req_count += 1

            json_response = response.json()
            if len(list(json_response.keys())) > 0:
                break

            retry_count -= 1
            if retry_count < 0:
                break
            print('ERROR: no response? Sleeping for 5 seconds and retrying...')
            time.sleep(5)

        response_listings = json_response.get('listings', [])
       
        # print('TEST parameters per listing: ', len(json_response.get('listings',[{}])[0].keys()))
        # test_string = f'TEST number of listings: returned: {len(json_response.get("listings", []))}; total: {json_response.get("totalResultCount")}'

        return response_listings #, test_string
       
    def run_subqueries(self, subqueries_params: list) -> list:
        print('Started running subqueries...')
        all_listings = []
        with Pool(processes=4) as pool:
            for listings in pool.map(self.make_request, subqueries_params):
                all_listings.extend(listings)
        print('Finished running subqueries...')
        return all_listings

if __name__ == "__main__":
    api = AutotraderAPI()
    api.run_query()
