import csv
import os
import requests
from requests import HTTPError
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
from collections import deque
from sqlalchemy.orm import sessionmaker
from models import Company, Person

load_dotenv()
WATERFALL_PROSPECT_ENDPOINT = 'https://api.waterfall.to/v1/prospector'
engine = create_engine(os.getenv("DATABASE_URL"))


def get_domains_from_csv(file_path):
    domains = []
    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            domains.append(row['domain'])
    return domains


def get_header():
    return {
        'x-waterfall-api-key': os.getenv('API_KEY'),
        'Content-Type': 'application/json'
    }


def launch_prospect(domain, title_filter):
    data = {
        'domain': domain,
        'title_filter': title_filter
    }
    try:
        response = requests.post(WATERFALL_PROSPECT_ENDPOINT, json=data, headers=get_header())
        response.raise_for_status()
        return response.json()
    except HTTPError as e:
        print("Error while launching prospect for domain {}".format(domain))
        print(e)
        return None


def find_prospect(job_id):
    try:
        response = requests.get(WATERFALL_PROSPECT_ENDPOINT + '/' + job_id, headers=get_header())
        response.raise_for_status()
        return response.json()
    except HTTPError as e:
        print("Error while fetching prospect for job id {}".format(job_id))
        print(e)
        return None


def write_company_contacts_to_csv(company, headers):
    company_domain = company['domain']
    rows = []
    with open(company_domain + '.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for row in company['persons']:
            row['phone_numbers'] = ";".join(row['phone_numbers'])
            rows.append(row)
        writer.writerows(rows)


def write_company_list_to_csv(company_list):
    headers = [
        "id", "first_name", "last_name", "linkedin_id", "linkedin_url",
        "personal_email", "location", "country", "company_id",
        "company_linkedin_id", "company_name", "company_domain",
        "professional_email", "mobile_phone", "phone_numbers", "title",
        "seniority", "department", "quality", "email_verified", "email_verified_status"
    ]
    for company in company_list:
        write_company_contacts_to_csv(company, headers)


def save_to_db(company_contact_list):
    Session = sessionmaker(bind=engine)
    session = Session()

    company_data = []
    contact = []
    for company_contact in company_contact_list:
        company_data.append(company_contact['company'])
        for contact_data in company_contact['persons']:
            contact = contact + contact_data

    try:
        session.bulk_insert_mappings(inspect(Company), company_data)
        session.bulk_insert_mappings(inspect(Person), contact)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def main(csv_file_path):
    csv_file_path = 'input.csv'
    title_filter = 'manager'

    domains = get_domains_from_csv(csv_file_path)

    job_ids = []
    for domain in domains:
        job_id = launch_prospect(domain, title_filter=title_filter)
        if job_id:
            print("Prospect launched for domain {} with job id {}".format(domain, job_id))
            job_ids.append(job_id)

    # FIFO Queue
    job_queue = deque(job_ids)

    company_contact_list = []
    while len(job_queue) > 0:
        job_id = job_queue.pop()
        result = find_prospect(job_id)
        if result:
            if result['status'] == 'RUNNING':
                job_queue.appendleft(job_id)
            elif result['status'] == 'SUCCEEDED':
                company_contact_list.append(result['output'])
            else:
                # will handle FAILED, TIMED_OUT, ABORTED statuses
                print("Prospect failed for job id {} with status {}".format(job_id, result['status']))

    write_company_list_to_csv(company_contact_list)
    save_to_db(company_contact_list)


if __name__ == '__main__':
    main('input_companies.csv', 'output_contacts.csv')
