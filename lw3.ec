#include <stdio.h>
#include <stdlib.h>
#include <sqlca.h>
#include <locale.h>

void do_error(char *st_name, int errnum)
{
    printf("Error code %d at %s \n", errnum, st_name);
    printf("Error message: %s\n", sqlca.sqlerrm.sqlerrmc);
    return;
}

bool Connect()
{
    // connect to database
    EXEC SQL connect to students @fpm2.ami.nstu.ru user "pmi-b8114" using "wec69Sik";
    if (sqlca.sqlcode != 0)
    {
        do_error("ERROR Connect to database", sqlca.sqlcode);
        return;
    }

    // connect to scheme
    EXEC SQL set search_path to pmib8117;

    if (sqlca.sqlcode != 0)
    {
        do_error("ERROR Set database", sqlca.sqlcode);
        return;
    }
}

int EXERCISE1()
{
    EXEC SQL begin declare section;
    int id_prod, countd;
    char name[30];
    char *task1;
    int ind;
    EXEC SQL end declare section;

    EXEC SQL begin work;

    printf("EXERCISE1\n");

    task1 = "select product.id_prod, product.name, s.countd "
            "from product "
            "left join ( "
            "select compound.id_prod prod, count(compound.id_dish) countd "
            "from compound "
            "group by compound.id_prod ) as s on product.id_prod=s.prod "
            "order by id_prod ";

    EXEC SQL prepare task1 from :task1;

    if (sqlca.sqlcode < 0)
    {
        do_error("ERROR while preparing request", sqlca.sqlcode);
        EXEC SQL rollback work;
        return -1;
    }

    EXEC SQL declare cursor_1 cursor for task1;
    EXEC SQL open cursor_1;
    if (sqlca.sqlcode < 0)
    {
        do_error("ERROR opening cursor", sqlca.sqlcode);
        EXEC SQL close cursor_1;
        EXEC SQL rollback work;
        return -1;
    }
    exec sql fetch next from cursor_1 into :id_prod, :name, :countd INDICATOR ind;
    if (sqlca.sqlcode < 0)
    {
        do_error("ERROR occurred while reading the cursor", sqlca.sqlcode);
        EXEC SQL close cursor_1;
        EXEC SQL rollback work;
        return -1;
    }
    else if (sqlca.sqlcode == 100)
        printf("There is no data");
    else
    {
        printf("\nResult:\nid_prod\tname\t\t\tcount\n");
        printf("%d\t%s\t\t\t", id_prod, name);
        if (ind == 0)
            printf("%d\n", countd);
        else
            printf("NULL\n");

        while (sqlca.sqlcode == 0)
        {
            exec sql fetch next from cursor_1 into :id_prod, :name, :countd INDICATOR ind;
            if (sqlca.sqlcode < 0)
            {
                do_error("ERROR occurred while reading the cursor", sqlca.sqlcode);
                EXEC SQL close cursor_1;
                EXEC SQL rollback work;
                continue;
            }
            else if (sqlca.sqlcode == 0)
            {
                printf("%d\t%s\t\t\t", id_prod, name);
                if (ind == 0)
                    printf("%d\n", countd);
                else
                    printf("NULL\n");
            }
        }
    }
    EXEC SQL close cursor_1;
    EXEC SQL commit work;
    return 0;
}

int EXERCISE2()
{
    EXEC SQL begin declare section;
    int id_dish, id_prod, cost;
    double weight;
    char *task2, *task2_1;
    int tmpCost;
    EXEC SQL end declare section;

    EXEC SQL begin work;

    printf("EXERCISE2\n");

    task2 = "select id_dish, weight, (select cost from cost_dyn r where r.id_dish=compound.id_dish and date<=CURRENT_DATE order by r.date DESC limit 1)"
            "from compound "
            "where id_prod=? ";

    task2_1 = "insert into cost_dyn values"
              "(nextval('cost_dyn_id_seq'::regclass), ?, CURRENT_DATE, ?); ";

    EXEC SQL prepare task2 from :task2;
    EXEC SQL prepare task2_1 from :task2_1;

    if (sqlca.sqlcode < 0)
    {
        do_error("ERROR while preparing request", sqlca.sqlcode);
        EXEC SQL rollback work;
        return -1;
    }

    int flag = 1;
    while (flag == 1)
    {
        printf("Enter id_prod: ");
        scanf("%d", &id_prod);

        EXEC SQL declare cursor_2 cursor for task2;
        EXEC SQL open cursor_2 using :id_prod;

        if (sqlca.sqlcode < 0)
        {
            do_error("ERROR opening cursor", sqlca.sqlcode);
            EXEC SQL close cursor_2;
            EXEC SQL rollback work;
            return -1;
        }

        exec sql fetch next from cursor_2 into :id_dish, :weight, :cost;
        if (sqlca.sqlcode < 0)
        {
            do_error("ERROR occurred while reading the cursor", sqlca.sqlcode);
            EXEC SQL close cursor_2;
            EXEC SQL rollback work;
            continue;
        }
        else 
            if (sqlca.sqlcode == 100)
                printf("There is no data");
            else
            {
                if (weight < 5)
                    tmpCost = cost * 0.9;
                else
                    tmpCost = cost * 0.85;

                EXEC SQL execute task2_1 using :id_dish, :tmpCost;
                if (sqlca.sqlcode < 0)
                {
                    do_error("ERROR task2_1", sqlca.sqlcode);
                    EXEC SQL close cursor_2;
                    EXEC SQL rollback work;
                    continue;
                }
                int t = sqlca.sqlcode;
                while (t == 0)
                {
                    exec sql fetch next from cursor_2 into :id_dish, :weight, :cost;
                    if (sqlca.sqlcode < 0)
                    {
                        do_error("ERROR occurred while reading the cursor", sqlca.sqlcode);
                        EXEC SQL close cursor_2;
                        EXEC SQL rollback work;
                        continue;
                    }
                    t = sqlca.sqlcode;

                    if (weight < 5)
                        tmpCost = cost * 0.9;
                    else
                        tmpCost = cost * 0.85;

                    EXEC SQL execute task2_1 using :id_dish, :tmpCost;
                    if (sqlca.sqlcode < 0)
                    {
                        do_error("ERROR task2_1", sqlca.sqlcode);
                        EXEC SQL close cursor_2;
                        EXEC SQL rollback work;
                        continue;
                    }
                }
            }
        EXEC SQL close cursor_2;
        printf("Do you want to continue executing the request? '1' - Yes, '0' - No\n");
        scanf("%d", &flag);
    }
    EXEC SQL commit work;
}


int EXERCISE3()
{
    EXEC SQL begin declare section;
    int id_dish;
    char name[50];
    char *task3;
    EXEC SQL end declare section;

    printf("EXERCISE3\n");

    task3 = "select dish.id_dish, dish.name "
            "from dish "
            "where dish.id_dish not in ( select distinct compound.id_dish "
            "from compound "
            "where compound.id_prod not in (select distinct supply.id_prod "
            "from supply "
            "join post on supply.id_post=post.id_post "
            "join town on post.id_town=town.id_town "
            "join country on town.id_country=country.id_country "
            "where country.name='Россия') ); ";

    EXEC SQL begin work;
    EXEC SQL prepare task3 from :task3;

    if (sqlca.sqlcode < 0)
    {
        do_error("ERROR occurred while preparing the request", sqlca.sqlcode);
        EXEC SQL rollback work;
        return -1;
    }
    EXEC SQL declare cursor_3 cursor for task3;
    EXEC SQL open cursor_3;
    if (sqlca.sqlcode < 0)
    {
        do_error("ERROR opening cursor", sqlca.sqlcode);
        EXEC SQL close cursor_3;
        EXEC SQL rollback work;
        return -1;
    }
    EXEC SQL fetch cursor_3 into :id_dish, :name;
    if (sqlca.sqlcode < 0)
    {
        do_error("ERROR occurred while reading the cursor", sqlca.sqlcode);
        EXEC SQL close cursor_3;
        EXEC SQL rollback work;
        return -1;
    }
    else if (sqlca.sqlcode == 100)
        printf("\nThere is no data.\n");
    else
    {
        printf("Result:\nid_dish\t\tname\n");
        printf("%d\t%s\n", id_dish, name);
        while (sqlca.sqlcode == 0)
        {
            EXEC SQL fetch cursor_3 into :id_dish, :name;
            if (sqlca.sqlcode < 0)
            {
                do_error("ERROR occurred while reading the cursor", sqlca.sqlcode);
                EXEC SQL close cursor_3;
                EXEC SQL rollback work;
                return -1;
            }
            else if (sqlca.sqlcode == 0)
                printf("%d\t%s\n", id_dish, name);
        }
    }
    EXEC SQL close cursor_3;

    EXEC SQL commit work;
}

int main()
{
    printf("Start\n");

    if (Connect())
    {
        printf("Error connect\n");
        return -1;
    }

    printf("Connect\n");

    int n, flag = 0;
    do
    {
        printf("\nPlease enter number:1-4:\n");
        scanf("%d", &n);
        switch (n)
        {
        case 1:
            EXERCISE1();
            break;

        case 2:
            EXERCISE2();
            break;

        case 3:
            EXERCISE3();
            break;

        case 4:
            flag = 1;
            EXEC SQL disconnect all;
            printf("Session closed\n");
            break;
        default:
            printf("Wrong number!\n");
            break;
        }
    } while (flag == 0);

    return;
}
