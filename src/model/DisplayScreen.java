/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package model;

/**
 *
 * @author brightoibe
 */
public interface DisplayScreen {
    public void showSuccess(String msg);
    public void showError(String msg);
    public void updateStatus(String msg);
    public void clear();
    public void updateProgress(final int i);
    public void updateMinMaxProgress(int min, int max);
    public void setState(boolean state);
        
    
    
}
