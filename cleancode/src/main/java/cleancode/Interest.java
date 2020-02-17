package cleancode;

public class Interest {
	double SimpleInterest(double Principle,double No_of_Years,double Rate)
    {
  	   return Principle*No_of_Years*Rate/100; 
    }
	double CompoundInterest(double Principle,double No_of_Years,double Rate,double No_of_times_Interest_applied){
 	   return Principle*Math.pow(1+Rate/(100*No_of_times_Interest_applied), No_of_times_Interest_applied*No_of_Years)-Principle;
 	  }
      }

        	             

