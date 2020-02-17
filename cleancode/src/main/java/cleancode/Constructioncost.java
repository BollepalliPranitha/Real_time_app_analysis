package cleancode;

public class Constructioncost {
                 double Cost(String Material_Standard ,double Total_area_of_house ,boolean Fully_automated_home ){
                	 //Total area in square feet
                	 if(Material_Standard=="Standard" && !Fully_automated_home)
                		 return 1200*Total_area_of_house;
                	 if(Material_Standard=="above Standard" && !Fully_automated_home)
                	     return 1500*Total_area_of_house;
                	 if(Material_Standard=="high Standard"){
                		 if(Fully_automated_home){
                			 return 2500*Total_area_of_house;
                		 }
                		 else
                		 return 1800*Total_area_of_house;}
                	 
                	 return 0;
                 }
}
