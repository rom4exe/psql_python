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
		CREATE TABLE IF NOT EXISTS phones (
		id_client INTEGER REFERENCES clients(id),
		phone VARCHAR(40) NOT NULL);
		""")

	conn.commit()
def add_client(conn, first_name, last_name, email, phones=None):
	if phones: phon = tuple((map(str, phones.split(', '))))

	with conn.cursor() as cur:
		cur.execute("""
		INSERT INTO clients (first_name, last_name, email)
		SELECT %s, %s, %s
		WHERE not exists
		(SELECT 1 FROM clients WHERE first_name = %s and last_name = %s and email = %s);
		""", (first_name, last_name, email, first_name, last_name, email))

		if phones:
			for p in phon:
				cur.execute("""
				INSERT INTO phones (id_client, phone)
				SELECT (SELECT id FROM clients WHERE first_name = %s and last_name = %s), %s
				WHERE not exists (SELECT 1 FROM phones
					WHERE id_client = (SELECT id_client
						FROM phones
						WHERE id_client = (SELECT id 
											FROM clients 
											WHERE first_name = %s
						AND last_name = %s)
					AND phone = %s)
				);
				""", (first_name, last_name, p, first_name, last_name, p,))
	conn.commit()
def add_phone(conn, id_client, phones):
	phon = tuple((map(str, phones.split(', '))))
	with conn.cursor() as cur:
		for p in phon:
			cur.execute("""
			INSERT INTO phones (id_client, phone)
			SELECT %s, %s
			WHERE not exists (select 1 from phones where id_client = %s AND phone = %s);
			""", (id_client, p, id_client, p,))


def change_client(conn, id_client, first_name=None, last_name=None, email=None, phones=None):
	m = {}
	p=''
	if first_name: m['first_name']= first_name
	if last_name: m['last_name']= last_name
	if email: m['email']= email
	with conn.cursor() as cur:
		for k, v in m.items():
			s = f"UPDATE clients SET {k} = %(v)s WHERE id = %(id_client)s;"
			cur.execute(s, {'v':v, 'id_client':id_client})
		if phones:
			phon = tuple((map(str, phones.split(', '))))
			cur.execute("""
			DELETE FROM phones
			WHERE id_client = %s;""", (id_client,))

			for p in phon:
				cur.execute("""
				INSERT INTO phones (id_client, phone)
				SELECT %s, %s
				WHERE not exists (select 1 from phones where id_client = %s AND phone = %s);
				""", (id_client, p, id_client, p,))


def delete_phone(conn, id_client, phone):
	with conn.cursor() as cur:
		cur.execute("""
		DELETE FROM phones
		WHERE id_client = %s AND phone  = %s;
		""", (id_client, phone,))

def delete_client(conn, id_client):
	with conn.cursor() as cur:
		cur.execute("""
		DELETE FROM phones
		WHERE id_client = %s;""", (id_client,))

		cur.execute("""
		DELETE FROM clients
		WHERE id = %s;
		""", (id_client,))


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
	prm1=()
	prm2=()
	if first_name:
		prm1 = (*prm1, first_name)
		prm2 = (*prm2,'first_name')
	if last_name:
		prm1 = (*prm1, last_name)
		prm2 = (*prm2, 'last_name')
	if email:
		prm1 = (*prm1, email)
		prm2 = (*prm2, 'email')
	soed =''
	usl = 'SELECT id, first_name, last_name, email, ARRAY_AGG(phone) \
			FROM clients c \
			JOIN phones p on p.id_client = c.id \
			WHERE'
	for s in prm2:
		usl=usl+soed
		usl += f" {s} = %s"
		soed = ' and '
		if s == prm2[-1]: usl+=' GROUP BY id'
	with conn.cursor() as cur:
		cur.execute(usl, prm1)
		res = (cur.fetchall())
		print(res)

with psycopg2.connect(database="clients_db", user="postgres", password="1") as conn:
	create_db(conn)
	add_client(conn, 'Roman', 'Safronov', 'rom.exe@mail.ru')
	add_client(conn, 'Ivan', 'Sergeev', 'sergiv@mail.ru', '89991597845, 89617411223')
	add_client(conn, 'Ivan', 'Petrov', 'petro@mail.ru', '89991697845, 83952640902')
	add_phone(conn, 1, '89501414388, 89501417470')
	delete_phone(conn, 1, '89501414388')
	delete_client(conn, 1)
	change_client(conn, 2,last_name = 'Sergeenko', phones = '89991597846, 89617411223')
	find_client(conn, first_name='Ivan')
conn.close()