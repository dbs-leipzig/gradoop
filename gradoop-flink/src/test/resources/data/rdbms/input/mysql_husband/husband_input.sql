INSERT INTO person (pnr,name) VALUES (0,'Peter');
INSERT INTO person VALUES (1,'Karla',0);
INSERT INTO person (pnr,name) VALUES (2,'Joachim');
INSERT INTO person VALUES (3,'Steffen',2);
INSERT INTO person (pnr,name) VALUES (4,'Michael');
INSERT INTO person VALUES (5,'Sven',4);

UPDATE person
	SET husband = 1
	WHERE pnr = 0;
	
UPDATE person
	SET husband = 3
	WHERE pnr = 2;

UPDATE person
	SET husband = 5
	WHERE pnr = 4;


