package com.company;

public class Item{
    double income;
    double longitude;
    double latitude;
    double rating;

    public Item(double income,  double latitude,double longitude, double rating) {
        this.income = income;
        this.longitude = longitude;
        this.latitude = latitude;
        this.rating = rating;
    }

    public Item(double income, double latitude,double longitude) {
        this.income = income;
        this.longitude = longitude;
        this.latitude = latitude;
    }
}