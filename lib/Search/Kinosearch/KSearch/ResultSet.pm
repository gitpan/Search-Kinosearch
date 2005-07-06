package Search::Kinosearch::KSearch::ResultSet;
use strict;
use warnings;

use attributes 'reftype';
use bytes;
use Carp;
use Inline 'C' => <<'ENDC';

/*#define PRINT_FUNCTION_NAMES 1*/

#ifdef PRINT_FUNCTION_NAMES
#define _debug_print_function(x) fprintf x
#else
#define _debug_print_function(x)
#endif

int newcount = 0;
int destroycount = 0;
int incrementor = 1;

typedef struct {
    int   bytes;
    char* name;
    char* str;
    char* source_ptr;
    char* dest_ptr;
} KDField;

typedef struct {
    long   num_hits;
    size_t num_pos;
    int    num_kdfields;
    long   rankelemsize;
    int    sort_finished;
    int    sort_by_score;
    
    KDField** kdfields;
    
    long*   doc_num;
    long*   score;
    short*  posaddr;
    long*   posdata;
    long*   poscalc;
    long*   sortedhits;
    
} ResultSet;


void _swap_endian(void*, size_t, int);
void _merge(char*, char*, long , long, long, long, long);
void _msort(char*, char*, long , long, long);
SV* new(char*, long, size_t);
void DESTROY(SV*);
long _get_num_hits(SV*);
size_t _get_num_pos(SV*);
void _set_doc_nums(SV*, SV*, long, long); 
void _set_posaddr(SV*, SV*, long);
void _set_posdata(SV*, SV*, size_t);
void _set_datetime(SV*, SV*, long);
void _add_up_scores( SV*, SV*, long , long );
void _dump(SV*, int);
SV* _modify_filterlist( SV*, SV*, char* );
void _apply_filterlist( SV*, SV*, char* );
void _filter_zero_scores( SV* );
int _merge_result_sets( SV*, SV*, SV* );
void _score_phrases (SV*, SV*);
void _expand_phrase_posdata ( SV*, int );
void _sort_hits( SV*, char* );
SV* _retrieve_hit_info ( SV*, long );    

void _copy_KDField_contents (KDField**, KDField**, int);
void _advance_KDField_temp_pointers ( KDField**, int, int ); 

void* _get_KDField (SV* obj, char* name ){
    _debug_print_function((stderr,"_get_KDField_pointer %d ", incrementor++));
    ResultSet* result_set = (ResultSet*)SvIV(SvRV(obj));
    int i;
    KDField** kdfields = result_set->kdfields;
    KDField* kdfield;
    for (i = 0; i < result_set->num_kdfields; i++) {
        kdfield = *kdfields;
        if (strcmp(kdfield->name, name) == 0) {
            return kdfield;
        }
        kdfields++;
    }
    fprintf(stderr, "Invalid field: %s\n", name);
    croak("");
}

void* _get_KDField_pointer (SV* obj, char* name ){
    KDField* kdfield = _get_KDField(obj, name);
    return (void*) kdfield->str;
}

KDField* _newKDField (char* name, int bytes, size_t num_elem, int zero_out) {
    _debug_print_function((stderr,"_newField %d ", incrementor++));
    KDField* kdfield;
    New(0, kdfield, 1, KDField);

    size_t len  = num_elem * bytes;

    kdfield->bytes = bytes;
    kdfield->name = savepv(name);
    if (zero_out) {
        Newz(0, kdfield->str, len, char);
    }
    else {
        New(0, kdfield->str, len, char);
    }
    return kdfield;
}

void _reset_temp_pointers(ResultSet* result_set) {
    _debug_print_function((stderr,"_reset_temp_pointers %d ", incrementor++));
    KDField** kdfields = result_set->kdfields;
    int i;
    KDField* kdfield;
    for (i = 0; i < result_set->num_kdfields; i++) {
        kdfield = *(kdfields + i);
        kdfield->source_ptr = kdfield->str;
        kdfield->dest_ptr = kdfield->str;
    }
}

/****************************************************************************
 * Constructor
 ****************************************************************************/
SV* new(char* class, long num_hits, size_t num_pos) {
    _debug_print_function((stderr,"new %d ", incrementor++));
    
    ResultSet* result_set;
    New(0, result_set, 1, ResultSet);
    
    result_set->num_hits   = num_hits;
    result_set->num_pos    = num_pos;
    result_set->sort_finished = 0;
    result_set->sort_by_score = 0;

    New(0, result_set->doc_num, num_hits, long);
    Newz(0, result_set->score,  num_hits, long);    /* zero out */
    New(0, result_set->posaddr, num_hits, short);
    New(0, result_set->posdata, num_pos, long);
    New(0, result_set->sortedhits, 1, long);        /* placeholder */
    New(0, result_set->poscalc, 1, long);           /* placeholder */
 
    New(0, result_set->kdfields, 1, KDField*);
    result_set->num_kdfields = 1;
    KDField** fields = result_set->kdfields;
    
    *fields = _newKDField("datetime", 8, num_hits, 0);

    fields++;
    
    SV* obj_ref = newSViv(0);
    SV* obj = newSVrv(obj_ref, class);
    sv_setiv(obj, (IV)result_set);
    SvREADONLY_on(obj);

    return obj_ref;
}

/****************************************************************************
 * Clean up.
 ****************************************************************************/
 
void DESTROY(SV* obj) {
    _debug_print_function((stderr,"destroy %d ", incrementor++));
    ResultSet* result_set = (ResultSet*)SvIV(SvRV(obj));
    int i;
    KDField** kdfields = result_set->kdfields;
    KDField* kdfield;
    for (i = 0; i < result_set->num_kdfields; i++) {
        kdfield = *(kdfields + i);
        Safefree(kdfield->name);
        Safefree(kdfield->str);
        Safefree(kdfield);
    }
    Safefree(result_set->doc_num);
    Safefree(result_set->posaddr);
    Safefree(result_set->posdata);
    Safefree(result_set->score);
    Safefree(result_set->sortedhits);
    Safefree(result_set->poscalc);
    Safefree(result_set);
}    

/****************************************************************************
 * Retrieve the number of documents in the result set.
 ****************************************************************************/
long _get_num_hits(SV* obj) {
    _debug_print_function((stderr,"get_num_hits %d ", incrementor++));
    return ((ResultSet*)SvIV(SvRV(obj)))->num_hits;
}

/****************************************************************************
 * Retrieve the number of token positions in the result set, which is the sum
 * of the number of token positions for each document in the result set.  If
 * there are two documents, and the term appears twice in one of them and once
 * in the other, _get_num_pos returns 3.
 ****************************************************************************/
size_t _get_num_pos(SV* obj) {
    _debug_print_function((stderr,"_get_num_pos %d ", incrementor++));
    return ((ResultSet*)SvIV(SvRV(obj)))->num_pos;
}

/****************************************************************************
 * Move a C array from a Perl scalar into a KDField structure.
 ****************************************************************************/
void _set_KDfield_str(SV* obj, char* fieldname, SV* input_sv, 
    long hit_num_offset, long add_to_each, int bytes, int allow_byteswap ) {
    _debug_print_function((stderr,"_set_KDfield %d ", incrementor++));
    ResultSet* result_set = (ResultSet*)SvIV(SvRV(obj));
    SV* kdata_sv = SvRV(input_sv);
    STRLEN kdata_len = SvCUR(kdata_sv);
    char* kdata_str = SvPV(kdata_sv, kdata_len);
    char* output_str;
    output_str = 
        (!strcmp(fieldname,"doc_num")) ? (char*) result_set->doc_num :
        (!strcmp(fieldname,"posaddr")) ? (char*) result_set->posaddr :
        (!strcmp(fieldname,"posdata")) ? (char*) result_set->posdata :
        (char*) _get_KDField_pointer(obj, fieldname);
    Move( kdata_str, (output_str + hit_num_offset*bytes), 
        kdata_len, char);
    if (_machine_is_little_endian() && allow_byteswap != 0) 
        _swap_endian(
            (output_str + hit_num_offset*bytes), kdata_len, bytes);
    if (add_to_each) {
        /* At present, the only time we'd want this is for doc_num */
        long* doc_num_ptr = (long*) (output_str + hit_num_offset*bytes);
        long i;
        long limit = kdata_len / bytes;
        for (i = 0; i < limit; i++) {
            *doc_num_ptr += add_to_each;
            doc_num_ptr++;
        }
    }
}

/****************************************************************************
 * Accumulate a score for each document. 
 ****************************************************************************/
void _add_up_scores( SV* obj, SV* input_sv, long factor, long hit_num_offset) {
    _debug_print_function((stderr,"_add_up_scores %d ", incrementor++));
    ResultSet* result_set = (ResultSet*)SvIV(SvRV(obj));
    SV* kdata_sv = SvRV(input_sv);
    STRLEN kdata_len = SvCUR(kdata_sv);
    unsigned char* scores = (unsigned char*) SvPV(kdata_sv, kdata_len);
    long* totals = result_set->score + hit_num_offset;
    
    long i;
    for (i = 0; i < kdata_len; i += 2) {
    /* Decode crude floating point format.  8 bit exponent, 8 bit mantissa, 
     * both non-negative. */
        *totals += (*(scores + 1) * factor) << *scores;
        totals++;
        scores += 2;
    }
}

/****************************************************************************
 * Pretty print the contents of the result set, for debugging purposes.
 ****************************************************************************/
void _dump(SV* obj, int fh_choice) {
    _debug_print_function((stderr,"_dump %d ", incrementor++));
    FILE* out_fh = fh_choice == 1 ? stderr : stdout;
    ResultSet* result_set = (ResultSet*)SvIV(SvRV(obj));
    long* doc_num = result_set->doc_num;
    short* posaddr = result_set->posaddr;
    long i, j, doc_pos;
    long num_hits = result_set->num_hits;
    long* posdata_pos = result_set->posdata;
    fprintf(out_fh, "\n=================================\n");
    fprintf(out_fh, "NUMHITS: %d\n", num_hits);
    fprintf(out_fh, "NUMPOS: %d\n", result_set->num_pos);
    for (i = 0; i < num_hits; i++) {
        fprintf(out_fh, "\nDOCNUM: %d SCORE: %d POSADDR: %hd\n\tPOSDATA: ", 
            *(doc_num + i),
            *(result_set->score + i),
            *(posaddr + i) );
        doc_pos = *(posaddr + i);
        for (j = 0; j < doc_pos; j++) {
            fprintf(out_fh, "%d ", *posdata_pos);
            posdata_pos++;
        }
    }
    fprintf(out_fh, "\n");
    /* This is garbage if you aren't sorting by score. */
    if (result_set->sort_finished) {
        fprintf(out_fh, "\tRANKELEMSIZE: %d\n\tSCORES: ", 
            result_set->rankelemsize);
        long* sortedhits_long = (long*) result_set->sortedhits;
        long offset = 0;
        long ind, hit_score;
        for (i = 0; i < num_hits; i++) {
            offset = i * (result_set->rankelemsize / 4 );
            ind = *(sortedhits_long + offset + (result_set->rankelemsize / 4 ) - 1);
            hit_score = *(sortedhits_long + offset);
            if (_machine_is_little_endian() && result_set->rankelemsize == 4) {
                _swap_endian(&hit_score, 4, 4);
            }
            fprintf(out_fh, "I: %d S: %d  ", ind, hit_score);
        }
    }
    fprintf(out_fh, "\n=================================\n");
}

/****************************************************************************
 * Test to see if the processor is little endian.
 ****************************************************************************/
int _machine_is_little_endian() {
    _debug_print_function((stderr,"_machine_is_little_endian %d ", incrementor++));
    long one= 1;
    return (*((char *)(&one)));
}

/****************************************************************************
 * Reverse byte order.
 ****************************************************************************/
void _swap_endian(void* input, size_t total_bytes, int width) {
    _debug_print_function((stderr,"_swap_endian %d ", incrementor++));
    char* addr = (char*) input;
    char tempchar;
    size_t i;
    if (width == 2) {
        for (i = 0; i < total_bytes; i += 2) {
            tempchar        = *(addr + i);
            *(addr + i)     = *(addr + i + 1);
            *(addr + i + 1) = tempchar;
           }
    }
    else if (width == 4) {
        for (i = 0; i < total_bytes; i += 4) {
            tempchar        = *(addr + i);
            *(addr + i)     = *(addr + i + 3);
            *(addr + i + 3) = tempchar;
            tempchar        = *(addr + i + 1);
            *(addr + i + 1) = *(addr + i + 2);
            *(addr + i + 2) = tempchar;
        }    
    }
    else {
        croak("_swap_endian only `deals with 2 and 4 byte formats");
    }
}

/****************************************************************************
 * Modify the contents of either a reqlist (containing doc_nums from a
 * production marked as required), or a neglist (containing doc_nums from a
 * production marked as negated.
 ****************************************************************************/
SV* _modify_filterlist( SV* obj, SV* filterlist_sv, char* modtype ) {
    _debug_print_function((stderr,"_modify_filterlist %d ", incrementor++));
    ResultSet* result_set = (ResultSet*)SvIV(SvRV(obj));

    if (!SvPOK(filterlist_sv)) 
 
        sv_setpvn(filterlist_sv, "\0", 0);

    long num_hits = result_set->num_hits;
    long* doc_nums = result_set->doc_num;
    STRLEN filterlist_len = SvCUR(filterlist_sv); 
    char* filter_nums_str = SvPV(filterlist_sv, filterlist_len);
    long* filter_nums = (long*) filter_nums_str;
    long num_filter_nums = filterlist_len / 4;
    
    int modtype_is_union;
    if (strcmp(modtype, "union") == 0) {
        modtype_is_union = 1;
    }
    else if (strcmp(modtype, "intersection") == 0) {
        modtype_is_union = 0;
    }
    else {
        croak("Internal error: modtype must be either intersection or union");
    }

    long i, j;
    STRLEN out_size;
    SV* out_sv;
    
    if (modtype_is_union) {
        /* might be bigger than necessary, but we only have to 
         * allocate once. */
        out_size = (num_filter_nums + num_hits) * 4;
    }
    else {
        out_size = num_filter_nums < num_hits ?
                    num_filter_nums : num_hits;
        out_size *= 4;
    }

    /* Return empty filterlist if either set is empty and the modtype is
     * intersection. */
    if (!modtype_is_union && ((num_filter_nums == 0) || (num_hits == 0))) {
        out_sv = newSVpvn("", 0);
        return out_sv;
    }
    
    /* Return filterlist unaltered if there's nothing in the result_set and
     * the modtype is union. */
    if (modtype_is_union && num_hits == 0) {
        SvREFCNT_inc(filterlist_sv);
        return filterlist_sv;
    }
    
    /* Return a copy of the doc_num array if the filterlist is empty and the
     * modtype is union. */
    if (modtype_is_union && num_filter_nums == 0) {
        char* doc_num_str = (char*) result_set->doc_num;
        out_sv = newSVpvn(doc_num_str, out_size);
        return out_sv; 
    }
    
    out_sv = newSV(out_size);
    SvPOK_on(out_sv);
    char* out_str = SvPV(out_sv, out_size);
    long* out_nums = (long*) out_str;
    
    j = 0;
    out_size = 0;
    if (modtype_is_union) {
        while (i < num_hits && j < num_filter_nums) {
            if (*filter_nums == *doc_nums) {
                *out_nums = *doc_nums;
                i++, j++, out_nums++, doc_nums++, filter_nums++;
            }
            else if (*doc_nums < *filter_nums) {
                *out_nums = *doc_nums;
                i++, out_nums++, doc_nums++;
            }
            else {
                *out_nums = *filter_nums;
                j++, out_nums++, filter_nums++;
            }
            out_size += 4;
        }
        /* We've exhausted either the filter_nums or the doc_nums at this
         * point, so only one of these while loops will be executed, max. */ 
        while (i < num_hits) {
            *out_nums = *doc_nums;
            i++, out_nums++, doc_nums++;
            out_size += 4;
        }
        while (j < num_filter_nums) {
            *out_nums = *filter_nums;
            j++, out_nums++, filter_nums++;
            out_size += 4;
        }
    }
    else {
        for (i = 0; i < num_hits; i++) {
            while (*filter_nums < *doc_nums && j < num_filter_nums) {
                j++, filter_nums++;
            }
            if (*filter_nums == *doc_nums) {
                *out_nums = *filter_nums;
                out_nums++;
                out_size += 4;
            }
            doc_nums++;
        }
    }
    SvCUR_set(out_sv, out_size);
    return out_sv;
}

/****************************************************************************
 * Winnow down a result set, either by eliminating documents on a neglist, or
 * eliminating documents missing from a reqlist.
 ****************************************************************************/
void _apply_filterlist( SV* obj, SV* filterlist_sv, char* filtertype ) {
    _debug_print_function((stderr,"_apply_filterlist %d ", incrementor++));
    ResultSet* result_set = (ResultSet*)SvIV(SvRV(obj));
    STRLEN filterlist_len = SvCUR(filterlist_sv);
    char* filterlist_str = SvPV(filterlist_sv, filterlist_len);
    long* filter_nums = (long*) filterlist_str;
    long* doc_nums = result_set->doc_num;
    long* scores = result_set->score;
    long i, j;
    long num_filtered = filterlist_len / 4;
    long num_doc_nums = result_set->num_hits;
    
    j = 0;
    if (strcmp(filtertype, "neg") == 0) {
        for (i = 0; i < num_doc_nums; i++) {
            while (*filter_nums < *doc_nums && j < num_filtered) {
                j++;
                filter_nums++;
            }
            if (*filter_nums == *doc_nums)
                *(scores + i) = 0;
            if (j >= num_filtered)
                break;
            doc_nums++;
        }
    }
    else if (strcmp(filtertype, "req") == 0) {
        if (num_filtered == 0) {
            for (i = 0; i < num_doc_nums; i++) {
                *(scores + i) = 0;
            }
        }
        else {
            for (i = 0; i < num_doc_nums; i++) {
                while (*filter_nums< *doc_nums && j < num_filtered) {
                    j++;
                    filter_nums++;
                }
                if (*filter_nums != *doc_nums)
                    *(scores + i) = 0;
                if (j >= num_filtered)
                    break;
                doc_nums++;
            }
        }
    }
    else {
        croak("filtertype must be either 'neg' or 'req'");
    }
}

/****************************************************************************
 * Take a result set which has had entries marked for deletion (by setting the
 * score to zero) and collapse it.  It's not necessary to reallocate space --
 * just move everything to the front and label the ResultSet object  with a 
 * different num_hits.
 ****************************************************************************/
void _filter_zero_scores( SV* obj ) {
    _debug_print_function((stderr,"_filter_zero_scores %d ", incrementor++));
    ResultSet* result_set = (ResultSet*) SvIV(SvRV(obj));
    
    long   num_hits = result_set->num_hits;
    long*  doc_num  = result_set->doc_num;
    long*  score    = result_set->score;
    short* posaddr  = result_set->posaddr;
    long*  posdata  = result_set->posdata;
    KDField** kdfields = result_set->kdfields;
    KDField* otherfield;
    int num_kdfields = result_set->num_kdfields;
    

    long i, j, k, x, termfreq;
    long out_i = 0;
    size_t out_j = 0;
    j = 0;
    for (i = 0; i < num_hits; i++) {
        termfreq = *(posaddr + i);
        
        if (*(score + i) != 0) {
            
            *(doc_num + out_i) = *(doc_num + i);
            *(score + out_i)   = *(score + i);
            *(posaddr + out_i) = *(posaddr + i);
            for (x = 0; x < num_kdfields; x++) {
                otherfield = *(kdfields + x);
                Move( (otherfield->str + i*otherfield->bytes), 
                      (otherfield->str + out_i*otherfield->bytes), 
                      otherfield->bytes, char);
            }
            for (k = 0; k < termfreq; k++) {
                *(posdata + out_j + k) = *(posdata + j + k);
            }
            
            /* keep track of where you are in output*/
            out_i++;
            out_j += termfreq;
        }
        /* keep track of where you are in source posdata */
        j += termfreq;
    }
    /* Tell the result_set its new size. */
    result_set->num_hits = out_i;
    result_set->num_pos = out_j;
}

/****************************************************************************
 * Combine the contents of two result sets.  
 ****************************************************************************/
int _merge_result_sets( SV* obj1, SV* obj2, SV* out_obj ) {
    _debug_print_function((stderr,"_merge_result_sets %d ", incrementor++));
    ResultSet* rs1 = (ResultSet*)SvIV(SvRV(obj1));
    ResultSet* rs2 = (ResultSet*)SvIV(SvRV(obj2));

    /* Short cut.  If either result set is empty, tell Perl to use the
     * non-empty result set.  */ 
    if (rs1->num_hits == 0) {
        return 2;
    }
    else if (rs2->num_hits == 0) {
        return 1;
    }

    long i = 0;
    long j = 0;

    int x = 0;
    int num_kdfields = rs1->num_kdfields;

    long num_hits1 = rs1->num_hits;
    long num_hits2 = rs2->num_hits;
    long* doc_num1 = rs1->doc_num;
    long* doc_num2 = rs2->doc_num;

    long out_num_hits = 0;

    ResultSet* out_set = (ResultSet*) SvIV(SvRV(out_obj));
    long* out_doc_num  = out_set->doc_num;
    long* out_score    = out_set->score;
    short* out_posaddr = out_set->posaddr;
    long* out_posdata  = out_set->posdata;
    KDField** out_kdfields = out_set->kdfields;
    KDField* out_kdfield;
  
    long* score1 = rs1->score;
    long* score2 = rs2->score;
    short* posaddr1  = rs1->posaddr;
    short* posaddr2  = rs2->posaddr;
    long* posdata1   = rs1->posdata;
    long* posdata2   = rs2->posdata;
    KDField** kdfields1 = rs1->kdfields;
    KDField** kdfields2 = rs2->kdfields;
    KDField* kdfield;
    int kdbytes;

    _reset_temp_pointers(rs1); 
    _reset_temp_pointers(rs2); 
    _reset_temp_pointers(out_set); 

    /* Fill up the merged set object. */
    long pos_i = 0;
    i = 0;
    j = 0;
    long out_i = 0;
    size_t out_pos_i = 0;
    while (i < num_hits1 && j < num_hits2) {
        if (*doc_num1 == *doc_num2) {
            *out_doc_num = *doc_num1;
            *out_score = *score1 + *score2;
            *out_posaddr = *posaddr1 + *posaddr2;
            for (pos_i = 0; pos_i < *posaddr1; pos_i++) {
                *out_posdata = *posdata1;
                posdata1++, out_posdata++;
                out_pos_i++;
            }
            for (pos_i = 0; pos_i < *posaddr2; pos_i++) {
                *out_posdata = *posdata2;
                posdata2++, out_posdata++;
                out_pos_i++;
            }
            _copy_KDField_contents (kdfields1, out_kdfields, num_kdfields);
            _advance_KDField_temp_pointers ( kdfields2, num_kdfields, 1); 
            i++, j++, doc_num1++, doc_num2++, score1++, score2++, 
                posaddr1++, posaddr2++;
        }
        else if (*doc_num1 < *doc_num2) {
            *out_doc_num = *doc_num1;
            *out_score = *score1;
            *out_posaddr = *posaddr1;
            for (pos_i = 0; pos_i < *posaddr1; pos_i++) {
                *out_posdata = *posdata1;
                posdata1++, out_posdata++;
                out_pos_i++;
            }
            _copy_KDField_contents (kdfields1, out_kdfields, num_kdfields);
            i++, doc_num1++, score1++, posaddr1++;
        }
        else {
            *out_doc_num = *doc_num2;
            *out_score = *score2;
            *out_posaddr = *posaddr2;
            for (pos_i = 0; pos_i < *posaddr2; pos_i++) {
                *out_posdata = *posdata2;
                posdata2++, out_posdata++;
                out_pos_i++;
            }
            _copy_KDField_contents (kdfields2, out_kdfields, num_kdfields);
            j++, doc_num2++, score2++, posaddr2++;
        }
        out_doc_num++, out_score++, out_posaddr++;
        out_i++;
    }
    while (i < num_hits1) {
        *out_doc_num = *doc_num1;
        *out_score = *score1;
        *out_posaddr = *posaddr1;
        for (pos_i = 0; pos_i < *posaddr1; pos_i++) {
            *out_posdata = *posdata1;
            posdata1++, out_posdata++;
            out_pos_i++;
        }
        _copy_KDField_contents (kdfields1, out_kdfields, num_kdfields);
        i++, doc_num1++, score1++, posaddr1++;
        out_doc_num++, out_score++, out_posaddr++;
        out_i++;
    }
    while (j < num_hits2) {
        *out_doc_num = *doc_num2;
        *out_score = *score2;
        *out_posaddr = *posaddr2;
        for (pos_i = 0; pos_i < *posaddr2; pos_i++) {
            *out_posdata = *posdata2;
            posdata2++, out_posdata++;
            out_pos_i++;
        }
        _copy_KDField_contents (kdfields2, out_kdfields, num_kdfields);
        j++, doc_num2++, score2++, posaddr2++;
        out_doc_num++, out_score++, out_posaddr++;
        out_i++;
    }
    out_set->num_hits = out_i;
    out_set->num_pos = out_pos_i;
    return 0;
}

void _copy_KDField_contents (KDField** source_fields, KDField** dest_fields,
                             int num_kdfields)
{
    _debug_print_function((stderr,"_copy_KDField_contents %d ", incrementor++));
    int x, kdbytes;
    KDField* source_field;
    KDField* dest_field;
    for (x = 0; x < num_kdfields; x++) {
        dest_field = *(dest_fields + x);
        source_field = *(source_fields + x);
        kdbytes = source_field->bytes;
        Move(source_field->source_ptr, dest_field->dest_ptr, kdbytes, char);
        source_field->source_ptr += kdbytes;
        dest_field->dest_ptr += kdbytes;
    }
}

void _advance_KDField_temp_pointers ( KDField** kdfields, int num_kdfields, 
                                      int advance_source_pointer) 
{
    _debug_print_function((stderr,"_advance_KDField_temp_pointers %d ", incrementor++));
    int x;
    if (advance_source_pointer) {
        for (x = 0; x < num_kdfields; x++) {
            (*(kdfields + x))->source_ptr += (*(kdfields + x))->bytes;
        }
    }
    else {
        for (x = 0; x < num_kdfields; x++) {
            (*(kdfields + x))->dest_ptr += (*(kdfields + x))->bytes;
        }
    }
}

/****************************************************************************
 * Given two result sets, modify the first so that it contains only documents
 * where the term1 (from set 1) and term2 (from set 2) occur sequentially. 
 ***************************************************************************/
void _score_phrases (SV* obj1, SV* obj2) {
    _debug_print_function((stderr,"_score_phrases %d ", incrementor++));
    ResultSet* rs1 = (ResultSet*)SvIV(SvRV(obj1));
    ResultSet* rs2 = (ResultSet*)SvIV(SvRV(obj2));

    long*  doc_num_out  = rs1->doc_num;
    long*  score_out    = rs1->score;
    short* posaddr_out;
    New(0, posaddr_out, rs1->num_hits, short);
    long*  posdata_out  = rs1->posdata;
    KDField** kdfields_out = rs1->kdfields;
    
    long   num_hits1 = rs1->num_hits;
    long*  doc_num1  = rs1->doc_num;
    long*  score1    = rs1->score;
    short* posaddr1  = rs1->posaddr;
    long*  posdata1  = rs1->posdata;
    KDField** kdfields1 = rs1->kdfields;
    
    long   num_hits2 = rs2->num_hits;
    long*  doc_num2  = rs2->doc_num;
    long*  score2    = rs2->score;
    short* posaddr2  = rs2->posaddr;
    long*  posdata2  = rs2->posdata;
    KDField** kdfields2 = rs2->kdfields;

    int num_kdfields = rs1->num_kdfields;
    _reset_temp_pointers(rs1); 
    _reset_temp_pointers(rs2); 

    long i = 0;
    long j = 0;
    long k;
    long out_i = 0;
    long out_pos_i = 0;
    long pos_i, pos_i_max, pos_j, pos_j_max;
    long token1pos, token2pos;

    for (i = 0; i < num_hits1; i++) {
        while (*doc_num2 < *doc_num1 && j < num_hits2) {
            posdata2 += *posaddr2;
            j++, doc_num2++, posaddr2++;
        }
        /* If one document contains both tokens... */
        if (*doc_num1 == *doc_num2) {
            pos_i = 0;
            pos_j = 0;
            pos_i_max = *posaddr1;
            pos_j_max = *posaddr2;

            int phrase_hits = 0;
            for(pos_i = 0; pos_i < pos_i_max; pos_i++) {
                token1pos = *(posdata1 + pos_i); 
                token2pos = *(posdata2 + pos_j);
                while (token2pos < (token1pos + 1) && pos_j < pos_j_max) {
                    pos_j++;
                    token2pos = *(posdata2 + pos_j);
                }
                /* If the tokens occur in order... */
                if (token2pos == token1pos + 1) {
                    /* We've matched the phrase... */
                    *(doc_num_out + out_i) = *doc_num1;
                    *(score_out + out_i) = 2* (*(score1 + i) + *(score2 + j));
                    /* If the phrase occurs more than once in this doc... */
                    if (phrase_hits) {
                        *(posaddr_out + out_i) += 1;
                    }
                    else {
                        *(posaddr_out + out_i) = 1;
                    }
                    *(posdata_out + out_pos_i) = token1pos;
                    out_pos_i++;
                    phrase_hits++;
                }
            }
            if (phrase_hits != 0) {
                out_i++; 
                _copy_KDField_contents(kdfields1, kdfields_out, num_kdfields);
            }
            else {
                _advance_KDField_temp_pointers(kdfields1, num_kdfields, 1);
            }
        }
        else {
            _advance_KDField_temp_pointers(kdfields1, num_kdfields, 1);
        }
        posdata1 += *posaddr1;
        doc_num1++;
        posaddr1++;
    }
    Safefree(rs1->posaddr);
    rs1->posaddr = posaddr_out;
    rs1->num_hits = out_i;
    rs1->num_pos = out_pos_i;
}

/****************************************************************************
 * After calling _score_phrases, the positional data stored in the result set
 * for the phrase only contains the location of the first token.  This
 * function adds additional locations up to the end of the phrase.
 ***************************************************************************/
void _expand_phrase_posdata ( SV* obj, int phraselength) {
    _debug_print_function((stderr,"_expand_phrase_posdata %d ", incrementor++));
    ResultSet* result_set = (ResultSet*) SvIV(SvRV(obj));
    short* posaddr_in = result_set->posaddr;
    long* posdata_in = result_set->posdata;
    long* posdata_out;
    long sizeofout = result_set->num_pos * phraselength; 
    New(0, posdata_out, sizeofout, long);
    result_set->posdata = posdata_out;
    
    long i, j, k, phrase_freq;
    long pos_i = 0;
    long out_pos_i = 0;
    for (i = 0; i < result_set->num_hits; i++) {
        phrase_freq = *(posaddr_in + i);
        *(posaddr_in + i) *= phraselength;
        for (j = 0; j < phrase_freq; j++) {
            for (k = 0; k < phraselength; k++) {
                *posdata_out = *(posdata_in + pos_i) + k;
                posdata_out++;
            }
            pos_i++;
        }
    }
    Safefree(posdata_in);
    result_set->num_pos = sizeofout;
}

/****************************************************************************
 * Sort a result set... sort of.
 * Actually, only a fixed length key-index string gets sorted.  This key index
 * string is used to derive sort position by _retrieve_hit_info.
 ***************************************************************************/
void _sort_hits( SV* obj, char* sortby ) {
    _debug_print_function((stderr,"_sort_hits %d ", incrementor++));
    ResultSet* result_set = (ResultSet*) SvIV(SvRV(obj));

    char* input_str;
    int width;
    if (strcmp(sortby, "score") == 0) {
        result_set->sort_by_score = 1;
        width = 4;
        input_str = (char*) result_set->score;
    }
    else {
        KDField* sortfield = _get_KDField(obj, sortby);
        width = sortfield->bytes;
        input_str = (char*) sortfield->str;
    }
    
    size_t input_bytes = result_set->num_hits * width;
    size_t total_bytes = result_set->num_hits * (width + 4);
    char* aux;
    New(0, aux, total_bytes, char);
    
    if (_machine_is_little_endian() && (!strcmp(sortby,"score"))) {
        /* Copy the input_str into aux, then change the pointer for input_str
         * to point at aux too.  */
        Move( input_str, aux, input_bytes, char ); 
        _swap_endian(aux, input_bytes, width);
        input_str = aux;
    }

    Safefree(result_set->sortedhits);
    char* out_str;
    New(0, out_str, total_bytes, char);
    result_set->sortedhits = (long*) out_str;
        
    long ind;
    long elemsize = width + 4;
    result_set->rankelemsize = elemsize;

    char* a = input_str;
    char* b = out_str;
    char* c = out_str;
    c += width;

    for (ind = 0; ind < result_set->num_hits; ind++ ) {
        Move(a, b, width, char);
        a += width;
        b += elemsize;
        Move(&ind, c, 1, long);
        c += elemsize;
    }

    _msort(out_str, aux, 0, (result_set->num_hits - 1), elemsize);
    
    Safefree(aux);
        
    long num_hits = result_set->num_hits;
    Safefree(result_set->poscalc);
    New(0, result_set->poscalc, num_hits, long);
    long* poscalc = result_set->poscalc;
    short* posaddr = result_set->posaddr;

    long i;
    *poscalc = 0;
    poscalc++;
    for (i = 1; i < num_hits; i++) {
        *poscalc = *(poscalc - 1) + *posaddr; 
        poscalc++; posaddr++;
    }
    result_set->sort_finished = 1;
}

/****************************************************************************
 * Merge sort.  The elements are byte arrays of fixed (but arbitrary) length.
 ***************************************************************************/
void _msort ( char* data, char* aux, 
              long left_top, long right_tail, 
              long elemsize ) 
{
    _debug_print_function((stderr,"_msort %d ", incrementor++));
    if (left_top < right_tail) {
        
        long left_tail = (left_top + right_tail) / 2;
        long right_top = left_tail + 1;
        
        _msort(data, aux, left_top,  left_tail,  elemsize);
        _msort(data, aux, right_top, right_tail, elemsize);
        
        _merge(data, aux, left_top, left_tail, right_top, right_tail,
               elemsize);
    }
}

/****************************************************************************
 * The merge part of the merge sort.
 ***************************************************************************/
void _merge( char* data,     char* aux, 
             long left_top,  long left_tail, 
             long right_top, long right_tail, 
             long elemsize) 
{
    _debug_print_function((stderr,"_merge %d ", incrementor++));
    long i, num_elements, posit, comparison;
    num_elements = (right_tail - left_top) + 1;
    posit = left_top;
    while ((left_top <= left_tail) && (right_top <= right_tail)) {
        comparison = memcmp( (data + (left_top * elemsize)), 
                             (data + (right_top * elemsize)),
                             elemsize );
        if (comparison != -1) {
            Move( (data + left_top*elemsize), (aux + posit*elemsize), 
                elemsize, char);
            left_top += 1;
            posit += 1;
        }
        else {
            Move( (data + right_top*elemsize), (aux + posit*elemsize), 
                elemsize, char);
            right_top += 1;
            posit += 1;
        }
    }
    while (left_top <= left_tail) {
        Move( (data + left_top*elemsize), (aux + posit*elemsize), 
            elemsize, char);
        left_top += 1;
        posit += 1;
    }
    while (right_top <= right_tail) {
        Move( (data + right_top*elemsize), (aux + posit*elemsize), 
            elemsize, char);
        right_top += 1;
        posit += 1;
    }
    
    for (i = 0; i < num_elements; i++) {
        Move( (aux + (right_tail * elemsize)), 
              (data + (right_tail * elemsize)), elemsize, char);
        right_tail--;
    }
}

/****************************************************************************
 * Fetch document number, score, positional data, and date time for the
 * document at a given rank.
 ***************************************************************************/
SV* _retrieve_hit_info ( SV* obj, long raw_hit_num ) {
    _debug_print_function((stderr,"_retrieve_hit_info %d ", incrementor++));
    ResultSet* result_set = (ResultSet*) SvIV(SvRV(obj));

    if (!result_set->sort_finished)
        croak("result set not yet sorted");
    long* sortedhits_long = (long*) result_set->sortedhits;
    long offset = ((raw_hit_num+1) * (result_set->rankelemsize / 4 )) -1;

    long ind = *(sortedhits_long + offset);
    
    long hit_score = *(result_set->score + ind);
/*
    if (_machine_is_little_endian() && result_set->sort_by_score)
        _swap_endian(&hit_score, 4, 4);
*/
    SV* hit_score_sv = newSViv(hit_score);
    
    long doc_num = *(result_set->doc_num + ind);
    SV* hit_doc_num_sv = newSViv(doc_num);

    long num_hit_pos = *(result_set->posaddr + ind);
    long i, tokenpos;
  
    long* hit_posdata = result_set->posdata + *(result_set->poscalc + ind);
    
    AV* token_positions = newAV();
    for (i = 0; i < num_hit_pos; i++) {
        tokenpos = *hit_posdata;
        SV* tokenpos_sv = newSViv(tokenpos);
        av_push(token_positions, tokenpos_sv);
        *hit_posdata++;
    }

    HV* hit_info = newHV();

    hv_store(hit_info, "score", 5, hit_score_sv, 0);
    hv_store(hit_info, "doc_num", 7, hit_doc_num_sv, 0);
    SV* token_positions_ref = newRV_noinc((SV*) token_positions);
    hv_store(hit_info, "positions", 9, token_positions_ref, 0);

    int x;
    for (x = 0; x < result_set->num_kdfields; x++) {
        KDField* kdfield = *(result_set->kdfields + x);
        SV* kdfield_sv = newSVpvn(kdfield->str, kdfield->bytes);
        int name_len = strlen(kdfield->name);
        hv_store(hit_info, kdfield->name, name_len, kdfield_sv, 0);
    }
    
    SV* hit_info_ref = newRV_noinc((SV*) hit_info);
    return hit_info_ref;
}

ENDC

1;

__END__
__POD__

=head1 NAME

Search::Kinosearch::KSearch::ResultSet - KSearch result set

=head1 SYNOPSIS

No public interface.

=head1 DESCRIPTION

Search::Kinosearch::KSearch::ResultSet is a helper module for
Search::Kinosearch::KSearch.  Do not use it by itself.

=head1 TO DO

=over

=item

Commenting.

=back

=head1 SEE ALSO

=over

=item

L<Search::Kinosearch|Search::Kinosearch>

=item

L<Search::Kinosearch::KSearch|Search::Kinosearch::KSearch>

=back

=head1 AUTHOR

Marvin Humphrey E<lt>marvin at rectangular dot comE<gt>
L<http://www.rectangular.com>

=head1 COPYRIGHT

Copyright (c) 2005 Marvin Humphrey.  All rights reserved.
This module is free software.  It may be used, redistributed and/or 
modified under the same terms as Perl itself.

=cut

