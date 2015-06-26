@@ -0,0 +1,42 @@
libname cw odbc dsn=cw schema=dbo;
libname pipeline odbc dsn=pipeline schema=dbo;
libname crap odbc dsn=loanad schema=dbo;
libname fm2 odbc dsn=famcii user='nrvuser' password= '{SAS002}2CB1260D0F86F52D426DA4701594B680' schema=dbo;
libname fm2sas '\\fmacfile\vol1\sas\output\fmii';
options validvarname=v7;

proc sql;
	create table frfsfees as
		select	a.seller_id,
				sum(b.field_serv_fee_amt) as frfstotal format=comma16.,
				sum(case when datepart(b.cped) >= intnx('month',date(),-12,'B') then b.field_serv_fee_amt else 0 end) as frfslast12mo format=comma16.
	from cw.view_loan_setup_sas a left join cw.tbl_Loan_Periodic_Information b on a.loan_number = b.loannum 
	group by a.seller_id order by a.seller_id;
quit;


proc sql;
	create table usdafsfees as
	select seller_id,sum(usdafstotal) as usdafstotal format=comma16.,sum(usdafslast12mo) as usdafslast12mo format=comma16.
	from
	(select b.famc_seller__ as seller_id,
				sum(a.beginning_balance*b.servicing_fee/12) as usdafstotal format=comma16.,
				sum(case when (a.dist_per_month) >= intnx('month',date(),-12,'B') then a.beginning_balance*b.servicing_fee/12 else 0 end) as usdafslast12mo format=comma16.
	from fm2sas.tbl_FMII_Monthly a left join fm2sas.tbl_FMII_Loan b on a.series_number = b.series_number where (a.dist_per_month) <= "01Jun2014"d 
	group by b.famc_seller__ 
	union
	select a.sellerid as seller_id,
				sum(b.begining_balance*a.servicingfee/12) as usdafstotal format=comma16.,
				sum(case when datepart(b.dist_per_month) >= intnx('month',date(),-12,'B') then b.begining_balance*a.servicingfee/12 else 0 end) as usdafslast12mo format=comma16.
	from fm2.tbl_FMII_LoanSetup a left join fm2.tbl_FMII_Periodic b on a.loannumber = b.loannumber where datepart(dist_per_month) > "01Jun2014"d 
	group by a.sellerid) c
	group by seller_id;
quit;

data sellerfeeincome; merge frfsfees (in=a) usdafsfees (in=b);
	by seller_id; 
	format allfees allfeeslast12mo comma16.;
	allfees = sum(usdafstotal,frfstotal);
	allfeeslast12mo = sum(usdafslast12mo,frfslast12mo);
	if seller_id ne "" and allfees > 0;
run;
