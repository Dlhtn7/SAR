package message_queues;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Copyright (C) 2023 Pr. Olivier Gruber                                    
 *                                                                       
 * This program is free software: you can redistribute it and/or modify  
 * it under the terms of the GNU General Public License as published by  
 * the Free Software Foundation, either version 3 of the License, or     
 * (at your option) any later version.                                   
 *                                                                       
 * This program is distributed in the hope that it will be useful,       
 * but WITHOUT ANY WARRANTY; without even the implied warranty of        
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         
 * GNU General Public License for more details.                          
 *                                                                       
 * You should have received a copy of the GNU General Public License     
 * along with this program.  If not, see <http://www.gnu.org/licenses/>. 
 */


public class CBroker extends Broker {


    public CBroker(String name) {
        super(name);
    }

    @Override
    public Channel accept(int port) {
        RendezVous rv = RendezVous.get(getName(), port);
        System.out.println(Thread.currentThread().getName() + " : accepting...");
        return rv.waitForAccept(this, port);
    }

    @Override
    public Channel connect(String name, int port) {
        RendezVous rv = RendezVous.get(name, port);
        System.out.println(Thread.currentThread().getName() + " : connecting...");
        return rv.waitForConnect(this, port);
    }

}
