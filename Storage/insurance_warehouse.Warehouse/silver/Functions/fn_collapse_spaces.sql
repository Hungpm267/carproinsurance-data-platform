-- ============================================================================
-- Helper: collapse multiple spaces into one
-- ============================================================================
CREATE   FUNCTION silver.fn_collapse_spaces(@input VARCHAR(4000))
RETURNS VARCHAR(4000)
AS
BEGIN
    DECLARE @result VARCHAR(4000) = LTRIM(RTRIM(@input));
    WHILE CHARINDEX('  ', @result) > 0
        SET @result = REPLACE(@result, '  ', ' ');
    RETURN @result;
END;