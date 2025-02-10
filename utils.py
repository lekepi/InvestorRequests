from datetime import timedelta
from models import session, TaskChecker, config_class, engine
from datetime import date
from email.message import EmailMessage
import smtplib
import base64

def encrypt_text(key, text):
    encrypted_chars = [chr(ord(char) ^ ord(key[i % len(key)])) for i, char in enumerate(text)]
    encrypted_text = ''.join(encrypted_chars)
    return base64.urlsafe_b64encode(encrypted_text.encode()).decode()


def decrypt_text(key, encrypted_text):
    encrypted_text = base64.urlsafe_b64decode(encrypted_text).decode()
    decrypted_chars = [chr(ord(char) ^ ord(key[i % len(key)])) for i, char in enumerate(encrypted_text)]
    return ''.join(decrypted_chars)


def task_checker_db(status, task_details, comment='', task_name='Get EMSX Trade', task_type='Task Scheduler ', only_new=False):
    if comment != '':
        comment_db = comment
    else:
        comment_db = 'Success'
    add_db = True
    if only_new:
        my_task = session.query(TaskChecker).filter(TaskChecker.task_details == task_details)\
                                            .filter(TaskChecker.status == status)\
                                            .filter(TaskChecker.active == 1).first()
        if my_task:
            add_db = False
    if add_db:
        new_task_checker = TaskChecker(
            task_name=task_name,
            task_details=task_details,
            task_type=task_type,
            status=status,
            comment=comment_db
        )
        session.add(new_task_checker)
        session.commit()

    if status == 'Success':
        session.query(TaskChecker).filter(TaskChecker.task_details == task_details) \
            .filter(TaskChecker.status == 'Fail').filter(TaskChecker.active == 1).delete()
        session.commit()


def last_alpha_date():
    today = date.today()
    if today.weekday() == 0:
        end_date = today - timedelta(days=3)
    else:
        end_date = today - timedelta(days=1)
    with engine.connect() as con:
        rs = con.execute("SELECT max(entry_date) FROM position WHERE alpha_usd is not NULL;")
        for row in rs:
            max_date = row[0]
        end_date = min(max_date, end_date)
    return end_date


def simple_email(subject, body, ml, html=None):

    mail = config_class.MAIL_USERNAME
    password = config_class.MAIL_PASSWORD

    msg = EmailMessage()
    msg['subject'] = subject
    msg['From'] = 'ananda.am.system@gmail.com'
    msg['To'] = ml  # multiple email: 'olivier@ananda-am.com, lekepi@gmail.com'
    msg.set_content(body)
    if html:
        msg.add_alternative(html, subtype='html')

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(mail, password)
        smtp.send_message(msg)


def clean_df_value(serie):
    if not serie.empty:
        return serie[0]
    else:
        return None


def find_future_date(my_date, days):
    cur_date = my_date
    nb_days = days
    while nb_days > 0:
        cur_date += timedelta(days=1)
        if cur_date.weekday() < 5:
            nb_days -= 1
    return cur_date


def find_past_date(my_date, days):
    cur_date = my_date
    nb_days = days
    while nb_days > 0:
        cur_date -= timedelta(days=1)
        if cur_date.weekday() < 5:
            nb_days -= 1
    return cur_date


def get_df_des(df):
    df_return = df[['a_1d', 'a_2d', 'a_3d', 'a_1w',
        'a_2w', 'a_4w', 'a_8w']].describe()
    df_return['Desc'] = df_return.index
    df_return = df_return[['Desc', 'a_1d', 'a_2d', 'a_3d', 'a_1w',
        'a_2w', 'a_4w', 'a_8w']]

    hr_1d = df[(df['a_1d'] >= 0)]['a_1d'].count() / df['a_1d'].count()
    hr_2d = df[(df['a_2d'] >= 0)]['a_2d'].count() / df['a_2d'].count()
    hr_3d = df[(df['a_3d'] >= 0)]['a_3d'].count() / df['a_3d'].count()
    hr_1w = df[(df['a_1w'] >= 0)]['a_1w'].count() / df['a_1w'].count()
    hr_2w = df[(df['a_2w'] >= 0)]['a_2w'].count() / df['a_2w'].count()
    hr_4w = df[(df['a_4w'] >= 0)]['a_4w'].count() / df['a_4w'].count()
    hr_8w = df[(df['a_8w'] >= 0)]['a_8w'].count() / df['a_8w'].count()

    df_return.loc['HitRatio'] = ['HitRatio', hr_1d, hr_2d, hr_3d, hr_1w, hr_2w, hr_4w, hr_8w]

    return df_return


def append_excel(df_des, title, ws):
    ws.append(title)
    headers = df_des.columns.tolist()
    rows = df_des.values.tolist()
    ws.append(headers)

    for row in rows:
        ws.append(row)