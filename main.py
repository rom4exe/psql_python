import psycopg2

def create_db(conn):
	with conn.cursor() as cur:
		cur.execute("""
		CREATE TABLE IF NOT EXISTS clients (id SERIAL PRIMARY KEY,
		first_name VARCHAR(40) NOT NULL,
		last_name VARCHAR(40) NOT NULL,
		email VARCHAR(40) NOT NULL);
		""")
		cur.execute("""
		CREATE TABLE IF NOT EXISTS phones (id SERIAL PRIMARY KEY,
		phone VARCHAR(40) NOT NULL);
		""")
		cur.execute("""
		CREATE TABLE IF NOT EXISTS clientsphones (
		id_client INTEGER REFERENCES clients(id),
		id_phone INTEGER REFERENCES phones(id),
		CONSTRAINT cp PRIMARY KEY (id_client, id_phone));
		""")
	conn.commit()
def add_client(conn, first_name, last_name, email, phones=None):
	if phones: phon = tuple((map(str, phones.split(', '))))

	with conn.cursor() as cur:
		cur.execute("""
		INSERT INTO clients (first_name, last_name, email)
		select %s, %s, %s
		where not exists
		(select 1 from clients where first_name = %s and last_name = %s and email = %s);
		""", (first_name, last_name, email, first_name, last_name, email))

		if phones:
			for p in phon:
				cur.execute("""
				INSERT INTO phones (phone)
				VALUES (%s);
				""", (p,))

				cur.execute("""
				INSERT INTO clientsphones (id_client, id_phone)
				VALUES ((SELECT id FROM clients WHERE (first_name = %s and last_name = %s)),
				(SELECT id FROM phones WHERE phone = %s));
				""", (first_name, last_name, p,))
	conn.commit()
def add_phone(conn, id_client, phones):
	phon = tuple((map(str, phones.split(', '))))
	with conn.cursor() as cur:
		for p in phon:
			cur.execute("""
			INSERT INTO phones (phone)
			select %s where not exists (select 1 from phones where phone = %s);
			""", (p,p,))

			cur.execute("""
			INSERT INTO clientsphones (id_client, id_phone)
			select (%s),
			(SELECT id FROM phones WHERE phone = %s)
			where not exists
			(select 1 from clientsphones where id_client = %s and id_phone = (SELECT id FROM phones WHERE phone = %s));
			""", (id_client, p, id_client, p,))


def change_client(conn, id_client, first_name=None, last_name=None, email=None, phones=None):
	m = {}
	if first_name: m['first_name']= first_name
	if last_name: m['last_name']= last_name
	if email: m['email']= email
	with conn.cursor() as cur:
		for k, v in m.items():
			cur.execute(f"\
			UPDATE clients\
			SET {k} = '{v}'\
			WHERE id = %s;\
			", (id_client,))

		if phones:
			phon = tuple((map(str, phones.split(', '))))
			cur.execute("""
			DELETE FROM clientsphones
			WHERE id_client = %s;
			""", (id_client,))

			cur.execute("""
			DELETE FROM phones
			WHERE id not in (SELECT id FROM phones
							INTERSECT SELECT id_phone FROM clientsphones);""")

			for p in phon:
				cur.execute("""
				INSERT INTO phones (phone)
				select %s where not exists (select 1 from phones where phone = %s);
				""", (p, p,))

				cur.execute("""
				INSERT INTO clientsphones (id_client, id_phone)
				select (%s),
				(SELECT id FROM phones WHERE phone = %s)
				where not exists
				(select 1 from clientsphones where id_client = %s and id_phone = (SELECT id FROM phones WHERE phone = %s));
				""", (id_client, p, id_client, p,))


def delete_phone(conn, id_client, phone):
	with conn.cursor() as cur:
		cur.execute("""
		DELETE FROM clientsphones
		WHERE id_phone = (SELECT id FROM phones WHERE phone = %s) and id_client = %s;
		""", (phone, id_client))

		cur.execute("""
		DELETE FROM phones
		WHERE phone  = %s;
		""", (phone,))

def delete_client(conn, id_client):
	with conn.cursor() as cur:
		cur.execute("""
		DELETE FROM clientsphones
		WHERE id_client = %s;
		""", (id_client,))

		cur.execute("""
		DELETE FROM phones
		WHERE id not in (SELECT id FROM phones
						INTERSECT SELECT id_phone FROM clientsphones);""")

		cur.execute("""
		DELETE FROM clients
		WHERE id = %s;
		""", (id_client,))


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
	m = {}
	if first_name: m['first_name'] = first_name
	if last_name: m['last_name'] = last_name
	if email: m['email'] = email
	soed =''
	usl = ''
	for s,r in m.items():
		usl=usl+soed
		usl+= f"{(''.join(s))} = '{(''.join(r))}'"
		soed = ' and '

	with conn.cursor() as cur:
		cur.execute(f"\
		SELECT * FROM clients\
		WHERE {usl};")
		res = list(cur.fetchone())

		cur.execute("""
		SELECT phone FROM phones
		WHERE id in (SELECT id FROM phones
					INTERSECT SELECT id_phone
								FROM clientsphones
								WHERE id_client = %s);
								""", (res[0],))
		res2 = cur.fetchall()
		res+=res2
		print(res)

with psycopg2.connect(database="clients_db", user="postgres", password="1") as conn:
	create_db(conn)
	add_client(conn, 'Roman', 'Safronov', 'rom.exe@mail.ru')
	add_client(conn, 'Ivan', 'Sergeev', 'sergiv@mail.ru', '89991597845, 89617411223')
	add_phone(conn, 1, '89501414388, 89501417470')
	delete_phone(conn, 1, '89501414388')
	delete_client(conn, 1)
	change_client(conn, 2, last_name = 'Sergeenko')
	find_client(conn, first_name='Ivan')
conn.close()