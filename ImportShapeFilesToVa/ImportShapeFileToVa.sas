/* The valib library is pre-assigned in the box where VA is intalled. */
/* The MAPSCSTM needs to be created in advance put the lib assignement in the appserver_autoexec_usermods.sas */ 

%LET mapname = %QUPCASE(&mapname.);
/* remove blank spaces */
%let &final_table. = %QUPCASE(%Qcmpres(&final_table.));

%put NOTE: Listing parameters;
%put NOTE: mapname = &mapname.;
%put NOTE: density = &density.;
%put NOTE: final_table = &final_table.;
%put NOTE: idcolumn = &idcolumn.;
%put NOTE: idcolumndesc = &idcolumndesc.;
%put NOTE: shp_file = &shp_file.;
%put NOTE: admintype = &admintype.;
%put NOTE: precision = &precision.;
%put NOTE: cleanUp = &cleanUp.;
%put NOTE: renderMap = &renderMap.;

/* add 1 (number) to the end of the table's name if needed */
%macro checkFinalTableName;
	%let lastChar=%substr(&final_table.,%length(&final_table.));
	%if &lastChar. ne 1 %then %do;
		%put NOTE:Final table renamed from &final_table. to &final_table.1;
		%let final_table = &final_table.1;
	%end;
%mend;
%checkFinalTableName;

/**************************************************************************** 
 * Step:            ERROR VALIDATION                                        *
 ****************************************************************************/ 
%macro checkError;
	%if (not %symexist(SQLRC)) %then %let SQLRC = 0;
	%let rc  = 0;
	%if &SYSERR. ne 0 %then %let rc = &SYSERR;
	%else %if &SQLRC gt 0 %then %let rc = &SQLRC;
	%else %if &SYSRC ne 0 %then %let rc = &SYSRC;
	
	%if &rc. ne 0 %then %do;
		%put ERROR: ABORTING...;
		%put SYSERR = &SYSERR.;
		%put SQLRC = &SQLRC.;
		%put SYSRC = &SYSRC.;
		%abort cancel;
	%end;
%mend checkError;

/**************************************************************************** 
 * Step:             BACKUP VA MAPPING TABLES                               *
 ****************************************************************************/ 
%macro backup;

	/* If ATTRLOOKUP_BKP does not exists, creates it and adds a new column - Date Time Stamp */
	%if (not %sysfunc(exist(valib.ATTRLOOKUP_BKP))) %then %do;     
		proc sql noprint;
	      create table valib.ATTRLOOKUP_BKP
		  	as
	         select t1.*, DATETIME() as datetimestamp format=datetime.
			 from valib.ATTRLOOKUP as t1;
		quit;
		%checkError;
	%end;
	%else %do;
		/* Add the datetimestamp column to the ATTRLOOKUP table */
		data attrlookup_tmp /view=attrlookup_tmp;
			set valib.ATTRLOOKUP;
			DATETIMESTAMP = datetime();
		run;
		%checkError;
		/*Append current data to the backup table*/
		proc append base = valib.ATTRLOOKUP_BKP 
		    data = attrlookup_tmp  force ; 
		run; 
		%checkError;
	%end;   

	/* If CENTLOOKUP_BKP does not exists, creates it and adds a new column - Date Time Stamp */
	%if (not %sysfunc(exist(valib.CENTLOOKUP_BKP))) %then %do;     
		proc sql noprint;
	      create table valib.CENTLOOKUP_BKP
		  	as
	         select t1.*, DATETIME() as datetimestamp format=datetime.
			 from valib.CENTLOOKUP as t1;
		quit;
		%checkError;
	%end;
	%else %do;
		/* Add the datetimestamp column to the CENTLOOKUP table */
		data CENTLOOKUP_tmp /view=CENTLOOKUP_tmp;
			set valib.CENTLOOKUP;
			DATETIMESTAMP = datetime();
		run;
		%checkError;
		/*Append current data to the backup table*/
		proc append base = valib.CENTLOOKUP_BKP 
		    data = CENTLOOKUP_tmp  force ; 
		run; 
		%checkError;
	%end;   
%mend backup;

/**************************************************************************** 
 * Step:             CLEAN UP PREVIOUS RUN FOR A GIVEN SHAPEFILE            *
 ****************************************************************************/ 
%macro cleanUp;
	%if &cleanUp. = Yes %then %do;
		%if (%sysfunc(exist(MAPSCSTM._IMPORT_LOG))) %then %do; 
			%let previousISO=; 
			/* get previous run */
			proc sql noprint;
				select iso,final_table into :previousISO,:previousFinalTable
				from MAPSCSTM._IMPORT_LOG 
				where shp_file = "&shp_file.";
			quit;
			%checkError;
			%if &previousISO. ne %then %do;
				/* delete rows */
				proc sql noprint;
					delete from valib.ATTRLOOKUP where iso = "&previousISO.";
					delete from valib.CENTLOOKUP where mapname = "&previousFinalTable.";
					delete from MAPSCSTM._IMPORT_LOG where shp_file = "&shp_file."; 
				quit;
				%checkError;
			/* drop the custom map table if it exists */
				proc datasets lib=%scan(&previousFinalTable.,1,%str(.)) nolist;
					delete %scan(&previousFinalTable.,2,%str(.));
				run;
				%checkError;
			%end;
		%end;
	%end;
%mend cleanUp;

/*==========================================================================* 
 * Step:            Register New Shape File                                 * 
 *==========================================================================*/ 
/* CREATE A LIST OF PREFIX*/
data prefix (keep=prefix);
	list = "a b c d e f g h i j k l m n o p q r s t u v w x y z";
	do i=1 to 26;
		do j=1 to 26;
			prefix = upcase(cats(scan(list,i),scan(list,j)));
			output;
			
		end;
	end;
run;
%checkError;
/* CREATE A LIST OF ISO VALUES*/
data ISO ;
	do i=1 to 999;
		ISO_CHAR = PUT(I,Z3.);
		output;
	end;
run;
%checkError;
/* GET THE FIRST PREFIX THAT DOES NOT EXISTS IN valib.ATTRLOOKUP - SAVE THE VALUE IN A SAS MACRO VAR */
proc sql noprint;
	select compress(min(prefix)) into :prefix
	from prefix 
	where not exists (select 1 from valib.ATTRLOOKUP where id = prefix or id1 = prefix);
quit;
%checkError;
/* GET THE LAST ISO THAT DOES NOT EXISTS IN valib.ATTRLOOKUP - SAVE THE VALUE IN A SAS MACRO VAR */
proc sql noprint;
	select compress(MAX(ISO_CHAR)) into :ISO
	from ISO 
	where not exists (select 1 from valib.ATTRLOOKUP where ATTRLOOKUP.ISO = ISO.ISO_CHAR);
quit;
%checkError;
%PUT NOTE: mapname=&mapname;
%PUT NOTE: ISO=&ISO.;
%PUT NOTE: PREFIX=&prefix.;


/**  Step end Register New Shape File **/

/*==========================================================================* 
 * Step:            PROC_MAPIMPORT                                          * 
 *==========================================================================*/ 

/* IMPORT SHAPEFILE TO A SAS DATA SET*/
proc mapimport out= mapimport_output
	datafile="&shp_file.";
	id &idcolumn.;
run;
%checkError;
proc sort data=mapimport_output;
	by &idcolumn.;
run;
%checkError;
data mapimport_output;
	set mapimport_output;
	by &idcolumn.;
	length _sk_internal 8;
	retain _sk_internal 0;
	if first.&idcolumn. then _sk_internal = _sk_internal + 1;
run;
%checkError;

/**  Step end PROC_MAPIMPORT **/

/*===========================================================================* 
 * Step:            PROC_GREDUCE                                             * 
 *===========================================================================*/ 

/* avoiding Unmatched Area Boundaries and also reduce the number the rows before run GREDUCE */
data pre_greduce;
	set mapimport_output;
	if x ne . then x = round(x,&precision.);
	if y ne . then y = round(y,&precision.);
run;
%checkError;
/*removing duplicated lines*/
proc sort data=pre_greduce noduprecs;
	by _sk_internal;
run;
%checkError;

/* Add the Density columns */
%macro runGReduce;
	%if &runGReduce. = Yes %then %do;
		proc greduce data=pre_greduce  out=greduce_output;
			id _sk_internal;
		run;
		%checkError;
	%end;
	%else %do;
		data greduce_output;
			set pre_greduce;
			density = &density.;
		run;
		%checkError;
	%end;
%mend runGReduce;
%runGReduce;


/* Render map to test different levels of density */
%macro renderMap;
	%if &renderMap.=Yes %then %do;
		proc sql noprint;
			select count(distinct _sk_internal) into :repeat 
			from greduce_output;
		quit;
		%do i=0 %to 6;
	/*	http://www2.sas.com/proceedings/sugi29/251-29.pdf*/
			goptions reset=all border;
			title1 "Render test density <= &i.";
			pattern v=e c=black repeat=&repeat.;
			proc gmap data=greduce_output
			          map=greduce_output density=&i.;
			   id _sk_internal;
			   choro _sk_internal / nolegend ;
			run;
			quit;
		%end;
	%end;
%mend renderMap;

%renderMap;

/**  Step end PROC_GREDUCE **/

/*==========================================================================* 
 * Step:            CREATE STANDARD CUSTOM MAP                              * 
 *==========================================================================*/ 

proc sql;
   create table STANDARD_CUSTOM_MAPPING as
      select
         (PROPCASE(&idcolumndesc.)) as IDLABEL length = 55,
         cats(compress("&PREFIX.-"),put(_sk_internal,best12.)) as ID length = 15,
         trim(PROPCASE(&idcolumndesc.)) as IDNAME length = 55,
         trim(PROPCASE("&mapname.")) as ID1NAME length = 55,
         "" as ID2NAME length = 55,
         compress("&ISO.") as ISO length = 3
            format = $3.
            informat = $3.,
         trim(UPCASE("&mapname.")) as ISONAME length = 55
            format = $44.
            informat = $44.,
         CATS(PROPCASE(trim(&idcolumndesc.))||"|"||PROPCASE(TRIM("&mapname."))) as key length = 300,
         COMPRESS("&PREFIX.") as ID1 length = 15,
         "" as ID2 length = 15,
         "" as ID3 length = 15,
         "" as ID3NAME length = 55,
         (1) as LEVEL length = 8,
         SEGMENT   
            label = 'ID segment number',
         X   
            label = 'Projected longitude coordinate',
         Y   
            label = 'Projected latitude coordinate',
         DENSITY   
            label = 'PROC GREDUCE density value',
         (1) as RESOLUTION length = 8
            label = 'Similar to Density, but processed for displaysize',
         X as LONG   
            label = 'Unprojected degrees longitude',
         Y as LAT   
            label = 'Unprojected degrees latitude',
         ("&admintype.") as ADMINTYPE length = 25
            label = 'cantons/ counties / districts / federal states / municipalities / peripheries / planning regions / provinces / regions / statistical regions',
         compress("&PREFIX.") as ISOALPHA2 length = 2
            label = 'ISO Alpha2-code for country',
         (0) as LAKE length = 5
            label = 'Lake Flag:1-water:2-citytype'
   from greduce_output
      where DENSITY <= &DENSITY.
   ;
quit;
%checkError;




/*==========================================================================* 
 * Step:            BACKUP                                                  * 
 *==========================================================================*/ 
/* Run the backup */

%backup;
%cleanUp;

/*==========================================================================* 
 * Step:            LOAD FINAL TABLE                                        * 
 *==========================================================================*/ 

/* Create table */
proc sql;
	create table MAPSCSTM.&final_table.
	as
	(
	select 
		ID
		,SEGMENT
		,LONG
		,LAT
		,X
		,Y
		,ISONAME
		,DENSITY
		,RESOLUTION
		,LAKE
		,ISOALPHA2
		,ADMINTYPE
		,IDNAME
	from STANDARD_CUSTOM_MAPPING
	);
quit;
%checkError;
/* Add constraint */
proc datasets library=MAPSCSTM nolist;
  modify &final_table.;
     ic create not null (ID);
quit;
%checkError;
/**  Step end Table Loader **/





/*==========================================================================* 
 * Step:            Prepare CENTLOOKUP for insert                           * 
 *==========================================================================*/ 
/*---- Delete existing rows for the same map in the target table  ----*/ 
proc sql;
  delete from valib.CENTLOOKUP
  where
     CENTLOOKUP.mapname = "MAPSCSTM.&final_table."   ;
quit;
%checkError;
/**  Step end Delete **/

/*==========================================================================* 
 * Step:            Insert Rows - Level 0 -into   CENTLOOKUP                * 
 *     (High hierarchy e.g Country)                                         *
 *==========================================================================*/ 

/*---- Insert rows into target table  ----*/ 
proc sql;
  insert into valib.CENTLOOKUP (mapname, ID, x, y)
  select
     "MAPSCSTM.&final_table." as mapname length = 41,
     T1.ID1 as ID length = 15,
     AVG(T1.X) as x length = 8   
        label = 'Longitude',
     AVG(T1.Y) as y length = 8   
        label = 'Latitude'
  from
     STANDARD_CUSTOM_MAPPING as T1
  group by  1,2;
quit;
%checkError;
/**  Step end Insert Rows - Level 0 **/

/*==========================================================================* 
 * Step:            Insert Rows - Level 1 -into   CENTLOOKUP                * 
 *     (Low hierarchy e.g County)                                           *
 *==========================================================================*/ 
/*---- Insert rows into target table  ----*/ 
proc sql;
  insert into valib.CENTLOOKUP (mapname, ID, x, y)
  select
     "MAPSCSTM.&final_table." as mapname length = 41,
     T1.ID length = 15,
     AVG(T1.X) as x length = 8   
        label = 'Longitude',
     AVG(T1.Y) as y length = 8   
        label = 'Latitude'
  from
     STANDARD_CUSTOM_MAPPING as T1
  group by  1,2
  ;
quit;
%checkError;
/**  Step end Insert Rows - Level 1 **/

/*==========================================================================* 
 * Step:            Prepare ATTRLOOKUP for insert                           * 
 *==========================================================================*/ 
/*---- Delete existing rows for the same map in the target table  ----*/ 
proc sql;
  delete from valib.ATTRLOOKUP
  where
     ATTRLOOKUP.ISO = compress("&ISO.");
quit;
%checkError;
/**  Step end Delete **/


/*==========================================================================* 
 * Step:            Insert Rows - Level 0 -into   ATTRLOOKUP                * 
 *     (High hierarchy e.g Country)                                         *
 *==========================================================================*/ 

/*---- Insert rows into target table  ----*/ 
proc sql;
  insert into valib.ATTRLOOKUP (IDLABEL, ID, IDNAME, ID1NAME, ID2NAME, ISO, ISONAME, key, ID1, ID2, ID3, ID3NAME, level)
  select distinct
     T1.ID1NAME as IDLABEL length = 55,
     compress("&PREFIX.") as ID length = 15,
     T1.ID1NAME as IDNAME length = 55,
     "" as ID1NAME length = 55,
     "" as ID2NAME length = 55,
     T1.ISO length = 3   
        format = $3.
        informat = $3.,
     T1.ISONAME length = 55   
        format = $44.
        informat = $44.,
     "" as key length = 300,
     "" as ID1 length = 15   
        label = 'Alpha2 Country code',
     "" as ID2 length = 15,
     "" as ID3 length = 15,
     "" as ID3NAME length = 55,
     0 as level length = 8
  from
     STANDARD_CUSTOM_MAPPING as T1
  ;
quit;
%checkError;  
/**  Step end Insert Rows - Level 0 **/

/*==========================================================================* 
 * Step:            Insert Rows - Level 1 -into   ATTRLOOKUP                * 
 *     (Low hierarchy e.g County)                                           *
 *==========================================================================*/ 


/*---- Insert rows into target table  ----*/ 
proc sql;
  insert into valib.ATTRLOOKUP (IDLABEL, ID, IDNAME, ID1NAME, ID2NAME, ISO, ISONAME, key, ID1, ID2, ID3, ID3NAME, level)
  select distinct
     T1.IDLABEL length = 55,
     T1.ID length = 15,
     T1.IDNAME length = 55,
     T1.ID1NAME length = 55,
     T1.ID2NAME length = 55,
     T1.ISO length = 3   
        format = $3.
        informat = $3.,
     T1.ISONAME length = 55   
        format = $44.
        informat = $44.,
     T1.key length = 300,
     T1.ID1 length = 15   
        label = 'Alpha2 Country code',
     T1.ID2 length = 15,
     T1.ID3 length = 15,
     T1.ID3NAME length = 55,
     T1.LEVEL length = 8
  from
     STANDARD_CUSTOM_MAPPING as T1
  ;
quit;
%checkError;

%macro importLog;
	/* If MAPSCSTM._IMPORT_LOG does not exists, creates it */
	%if (not %sysfunc(exist(MAPSCSTM._IMPORT_LOG))) %then %do;     
		data MAPSCSTM._IMPORT_LOG;
			length mapname $ 50 ISO $ 3 PREFIX $ 2 density 8 final_table $ 41 idcolumn $ 32 idcolumndesc $ 32
					shp_file $ 300 admintype $ 30 precision 8 cleanUp $ 3;
			label mapname = "Map Name"
				ISO = "ISO"
				PREFIX = "Prefix"
				density = "Density"
				final_table = "Final Table"
				idcolumn = "Unit Area Id"
				idcolumndesc = "Unit Area Name/Description"
				shp_file = "Shape File Path"
				admintype = "Administration Type"
				precision = "Precision"
				cleanUp = "Clean Up";
			stop;
		run;
		%checkError;
	%end;
	data _IMPORT_LOG;
		mapname = trim("&mapname.");
		ISO=compress("&ISO.");
		PREFIX=compress("&prefix.");
		density = &density.;
		final_table = compress("MAPSCSTM.&final_table.");
		idcolumn = compress("&idcolumn.");
		idcolumndesc = compress("&idcolumndesc.");
		shp_file = trim("&shp_file.");
		admintype = trim("&admintype.");
		precision = &precision.;
		cleanUp = compress("&cleanUp"); 
	run;
	proc append base=MAPSCSTM._IMPORT_LOG data=_IMPORT_LOG force nowarn;
	run;
	proc print data=MAPSCSTM._IMPORT_LOG noobs label;
		title "Import history";
	run;
	title "";
%mend importLog;
%importLog;
