# Analytical-SQL
RFM Analysis 
with c as (
select distinct customer_id, count(*) over (partition by customer_id) as number_of_orders,
sum(quantity*price) over (partition by customer_id) as sum_cost_for_each_customer,
round(MAX(TO_DATE(invoicedate, 'MM/DD/YYYY HH24:MI')) OVER() - MAX(TO_DATE(invoicedate, 'MM/DD/YYYY HH24:MI')) OVER(PARTITION BY customer_id)) AS Recency
from tableretail),
c1 as (
select DISTINCT customer_id,--
   
       ntile(5) over (order by number_of_orders) as rfm_frequency,
        ntile(5) over (order by sum_cost_for_each_customer) as rfm_monetary
       ,ntile(5) over (order by RECENCY) as r_score
       , (ntile(5) over (order by number_of_orders) + ntile(5) over (order by sum_cost_for_each_customer))/2 AS fm_score 
       from c
       )
       select
                     DISTINCT C.customer_ID, C.Recency,C1.rfm_frequency,C1.rfm_monetary, C1.r_score, C1.fm_score
                     ,CASE 
                                    WHEN((R_SCORE = 5) AND (FM_SCORE  =5 )) or ((R_SCORE = 5) AND (FM_SCORE  =4 )) or((R_SCORE = 4) AND (FM_SCORE  =4 ))
                                    THEN 'Champions'
                                    WHEN ( (R_SCORE = 5) AND (FM_SCORE  =2 ) ) or ( (R_SCORE = 4) AND (FM_SCORE  =2 )) or ( (R_SCORE = 3) AND (FM_SCORE  =3 ))or ( (R_SCORE = 4) AND (FM_SCORE  =3 ))  
                                    THEN 'Potential Loyalists'
                                    WHEN ( (R_SCORE = 5) AND (FM_SCORE  =3)) or ( (R_SCORE = 4) AND (FM_SCORE  =4)) or ( (R_SCORE = 3) AND (FM_SCORE  =5 )) or ( (R_SCORE = 3) AND (FM_SCORE  =4 )) 
                                    THEN 'Loyal Customers'
                                    WHEN (R_SCORE = 5) AND (FM_SCORE  =1 )
                                    THEN 'Recent Customers'
                                    WHEN ((R_SCORE = 4) AND (FM_SCORE  =1)) or ( (R_SCORE = 3) AND (FM_SCORE  =1))
                                    THEN 'Promising'
                                    WHEN ( (R_SCORE = 3) AND (FM_SCORE  =2)) or ( (R_SCORE = 2) AND (FM_SCORE  =3)) or ( (R_SCORE = 2) AND (FM_SCORE  =2))
                                    THEN ' Customers Needing Attention'
                                    WHEN ((R_SCORE = 2) AND (FM_SCORE  =5 )) or ( (R_SCORE = 2) AND (FM_SCORE  =4 )) or ( (R_SCORE = 1) AND (FM_SCORE  =3))
                                    THEN 'At Risk'
                                    WHEN ((R_SCORE = 1) AND (FM_SCORE  =5 )) or ((R_SCORE = 1) AND (FM_SCORE  =4)) 
                                    THEN 'cant lose them'
                                    WHEN (R_SCORE = 1) AND (FM_SCORE  =5 ) 
                                    THEN 'Hibernating '
                                    WHEN (R_SCORE =1) AND (FM_SCORE =2) 
                                    THEN 'Hibernating'
                                    WHEN (R_SCORE =1) AND (FM_SCORE =1 )
                                    THEN 'Lost'
                                    else 'other'
                                    END AS rfm_segment

              from  c,c1
              WHERE C.CUSTOMER_ID=C1.CUSTOMER_ID
              ORDER BY CUSTOMER_ID;
