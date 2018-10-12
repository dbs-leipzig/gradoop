CREATE TABLE IF NOT EXISTS person(
	pnr INT PRIMARY KEY,
	name VARCHAR(128),
	gatte INT,
	FOREIGN KEY (gatte) REFERENCES person(pnr)
);

INSERT INTO person (pnr,name) VALUES (0,'Peter');
INSERT INTO person VALUES (1,'Karla',0);
INSERT INTO person (pnr,name) VALUES (2,'Joachim');
INSERT INTO person VALUES (3,'Steffen',2);
INSERT INTO person (pnr,name) VALUES (4,'Sandra');
INSERT INTO person VALUES (5,'Petra',4);

UPDATE person
	SET gatte = 1
	WHERE pnr = 0;
	
UPDATE person
	SET gatte = 3
	WHERE pnr = 2;

UPDATE person
	SET gatte = 5
	WHERE pnr = 4;




