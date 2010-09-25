/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package crush;

/**
 *
 * @author edward
 */
public class CrushException extends Exception{
    public CrushException(){
        super();
    }
    public CrushException(String s){
        super(s);
    }
    public CrushException(String s,Exception e){
        super(s,e);
    }
}
